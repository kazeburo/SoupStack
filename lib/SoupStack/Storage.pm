package SoupStack::Storage;

use strict;
use warnings;
use 5.10.0;
use KyotoCabinet;
use Data::Validator;
use Fcntl qw/:DEFAULT :flock :seek/;
use File::Copy;
use Cache::LRU;
use IO::File;
use Mouse;

use Log::Minimal;
$Log::Minimal::AUTODUMP = 1;

has 'root' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has 'max_file_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1_000_000_000 
);

has 'fh_cache' => (
    is => 'ro',
    isa => 'Cache::LRU',
    lazy_build => 1,
);

sub _build_fh_cache {
    my $self = shift;
    Cache::LRU->new( size => 20 );
}

__PACKAGE__->meta->make_immutable();

our $OBJECT_HEAD_OFFSET = 8;
our $STACK_HEAD_OFFSET = 4;
our $READ_BUFFER = 2*1024*1024;

sub index_db {
    my $self = shift;
    return $self->{_index_db} if $self->{_index_db};
    my $path = $self->root . "/index.kch";
    my $db = KyotoCabinet::DB->new;
    $db->open($path, $db->OWRITER | $db->OCREATE) or die $db->error;
    $self->{_index_db} = $db;
    $db;
}

sub stack_db {
    my $self = shift;
    my ($index, $rid) = @_;

    my $key = sprintf "stack_%010d_%s.kch", $index, $rid;
    my $cached = $self->fh_cache->get($key);
    return $cached if $cached;

    my $path = $self->root . '/' . $key;
    my $db = KyotoCabinet::DB->new;
    $db->open($path, $db->OWRITER | $db->OCREATE) or die $db->error;
    $self->fh_cache->set($key,$db);
    $db;
}

sub find_stack_index {
    my ($self,$id) = @_;
    my $index = $self->index_db->get(KyotoCabinet::hash_murmur($id));
    return if ! defined $index;
    $index
}

sub find_pos {
    my $self = shift;
    my $args = shift;

    my $key = KyotoCabinet::hash_murmur($args->{id});

    my $pos = $self->stack_db(
        $args->{index},
        $args->{rid}
    )->get($key);

    return if ! defined $pos;
    my @pos = unpack "QQ",$pos;
    return {
        offset => $pos[0],
        size => $pos[1],
    }
}

sub put_pos {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Str',
        index => 'Int',
        rid => 'Int',
        offset => 'Int',
        size => 'Int',
    );
    my $args = $rule->validate(@_);
    my $key = KyotoCabinet::hash_murmur($args->{id});
    my $packed = pack "QQ",$args->{offset}, $args->{size};
    $self->index_db->set($key, $args->{index}) or die $self->db->error;
    $self->stack_db(
        $args->{index},
        $args->{rid},
    )->set($key, $packed) or die $self->db->error;
}

sub delete_pos {
    my ($self,$id) = @_;
    $self->index_db->remove(KyotoCabinet::hash_murmur($id));
}

sub stack_index {
    my $self = shift;
    SoupStack::Storage::StackIndex->new( root => $self->root );
}

sub open_stack {
    my ($self,$index, $create) = @_;
    my $key = sprintf "stack_%010d", $index;
    my $path = $self->root . '/' . $key;
    my $cached = $self->fh_cache->get($key);
    if ( $cached ) {
        my @stat = stat $path;
        return $cached if $cached->{ctime} == $stat[10];
    }

    my $fh;
    my $rid;
    if ( $create ) {
        sysopen( $fh, $path, O_RDWR|O_CREAT|O_EXCL ) or die $!;
        $rid = time;
        syswrite($fh, pack('L',$rid), $STACK_HEAD_OFFSET) or die $!;
    }
    else {
        sysopen( $fh, $path, O_RDWR ) or die $!;
        sysread( $fh, $rid, $STACK_HEAD_OFFSET) // die $!;
        $rid = unpack('L',$rid);
    }

    my @stat = stat $path;
    my $stack = { rid => $rid, fh => $fh, ctime => $stat[10] };
    $self->fh_cache->set( $key, $stack);
    $stack;
}

sub get {
    my ($self,$id) = @_;

    my $index = $self->find_stack_index($id);
    return unless $index;

    my $stack = $self->open_stack($index);

    my $pos = $self->find_pos({
        id => $id,
        index => $index,
        rid => $stack->{rid},
   });
    return unless $pos;

    sysseek( $stack->{fh}, $pos->{offset} + $OBJECT_HEAD_OFFSET,  SEEK_SET ) or die $!;
    my $size = $pos->{size};
    
    my $buffer='';
    my $perlbuf = ( $size <= $READ_BUFFER ) ? 1 : 0;
    if (!$perlbuf) {
        $buffer = IO::File->new_tmpfile;
        $buffer->binmode;
    }
    while ( $size ) {
        my $len = ( $size > $READ_BUFFER ) ? $READ_BUFFER : $size;
        my $readed = sysread( $stack->{fh}, my $read, $len);
        die $! if ! defined $readed;
        $size = $size - $readed;
        if ( $perlbuf ) {
            $buffer .= $read;
        }
        else {
            print $buffer $read;
        }
    }

    my $buf;
    if ( $perlbuf ) {
        open( $buf, '<', \$buffer);
        bless $buf, 'FileHandle';        
    }
    else {
        $buf = $buffer;
    }

    seek($buf,0,0);
    $buf;
}

sub delete {
    my ($self,$id) = @_;
    $self->delete_pos($id);
    return 1;
}

sub put {
    my $self = shift;
    my ($id,$fh) = @_;

    my $size = sysseek( $fh, 0, SEEK_END );
    sysseek($fh, 0, SEEK_SET );

    die 'cannot store size > max_file_size' 
        if $size + $OBJECT_HEAD_OFFSET >= $self->max_file_size;

    my $stack_index = $self->stack_index; #with lock
    my $index = $stack_index->id;
    my $create =  ( $index == 0 ) ? do { $index = $stack_index->incr; 1 } : 0;
    my $stack = $self->open_stack($index, $create);

    my $offset = sysseek( $stack->{fh}, 0, SEEK_END ) // die $!;
    $offset += 0;
    if ( $offset + $size > $self->max_file_size ) {
        $offset = 0;
        $index = $stack_index->incr;
        $stack = $self->open_stack($index,1);
    }
    my $len = syswrite($stack->{fh}, pack('q',KyotoCabinet::hash_murmur($id)), $OBJECT_HEAD_OFFSET) or die $!;
    die "couldnt write object header" if $len < 8;
    copy( $fh, $stack->{fh} ) or die $!;

    $self->put_pos({
        id => $id,
        index => $index,
        rid => $stack->{rid},
        offset => $offset,
        size => $size,
    });

    1;
}

1;

package SoupStack::Storage::StackIndex;

use strict;
use warnings;
use Mouse;
use Fcntl qw/:DEFAULT :flock :seek/;

has 'root' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

sub BUILD {
    my $self = shift;
    my $path = $self->root ."/stack.index";
    sysopen( my $fh, $path, O_RDWR|O_CREAT ) or die "Couldnt open lockfile: $!";
    flock( $fh, LOCK_EX ) or die "Couldnt get lock: $!";
    $self->{_fh} = $fh;

    sysread( $fh, my $id, 32) // die $!;

    if ( !$id ) {
        $id = 0;
        sysseek( $self->{_fh}, 0, SEEK_SET) or die $!;
        syswrite( $self->{_fh}, $id, length($id) ) or die $!;
    }

    $self->{_id} = $id;
}

sub id {
    shift->{_id};
}

sub incr {
    my $self = shift;
    $self->{_id}++;
    sysseek( $self->{_fh}, 0, SEEK_SET) or die $!;
    syswrite( $self->{_fh}, $self->{_id}, length($self->{_id}) ) or die $!;
    return $self->{_id};
}

sub DEMOLISH {
    my $self = shift;
    return if !$self->{_fh};
    flock( $self->{_fh}, LOCK_UN ) or die "$!";
}

__PACKAGE__->meta->make_immutable();
1;

