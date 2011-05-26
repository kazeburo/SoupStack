package SoupStack::Storage;

use strict;
use warnings;
use 5.10.0;
use Mouse;
use KyotoCabinet;
use Data::MessagePack;
use Data::Validator;
use Fcntl qw/:DEFAULT :flock :seek/;
use File::Copy;
use Cache::LRU;
use Plack::TempBuffer;

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
    Cache::LRU->new( size => 16 );
}

__PACKAGE__->meta->make_immutable();

our $OBJECT_HEAD_OFFSET = 8;
our $READ_BUFFER = 1024*1024;

sub db {
    my $self = shift;
    return $self->{_db} if $self->{_db};
    my $path = $self->root . "/stack.kch";
    my $db = KyotoCabinet::DB->new;
    $db->open($path, $db->OWRITER | $db->OCREATE) or die $db->error;
    $self->{_db} = $db;
    $db;
}

sub find_pos {
    my ($self,$id) = @_;
    my $pos = $self->db->get($id);
    return if ! defined $pos;
    Data::MessagePack->unpack($pos);
}

sub put_pos {
    my $self = shift;
    my $args = shift;
    my $pos = Data::MessagePack->pack({
        stack => $args->{stack},
        offset => $args->{offset},
        size => $args->{size},
    });
    $self->db->set($args->{id}, $pos) or die $self->db->error;
}

sub delete_pos {
    my ($self,$id) = @_;
    $self->db->remove($id);
}

sub stack_index {
    my $self = shift;
    SoupStack::Storage::StackIndex->new( root => $self->root );
}

sub open_stack {
    my ($self,$index) = @_;
    my $fh_cache = $self->fh_cache->get($index);
    return $fh_cache if $fh_cache;
    sysopen( my $fh, sprintf("%s/stack_%010d",$self->root,$index), O_RDWR|O_CREAT ) or die $!;
    $self->fh_cache->set( $index, $fh);
    $fh;
}

sub get {
    my ($self,$id) = @_;
    my $pos = $self->find_pos($id);
    return unless $pos;

    my $fh = $self->open_stack($pos->{stack});
    sysseek( $fh, $pos->{offset} + $OBJECT_HEAD_OFFSET, SEEK_SET ) or die $!;

    my $size = $pos->{size};
    
    my $buf = Plack::TempBuffer->new($size);
    while ( $size ) {
        my $len = ( $size > $READ_BUFFER ) ? $READ_BUFFER : $size;
        my $readed = sysread( $fh, my $read, $len);
        die $! if ! defined $readed;
        $size = $size - $readed;
        $buf->print($read);
    }

    $buf->rewind;
}

sub delete {
    my ($self,$id) = @_;
    $self->delete_pos($id);
    return 1;
}

sub put {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Str',
        fh => 'GlobRef'
    );
    my $args = $rule->validate(@_);
    my ($id,$fh) = map { $args->{$_} } qw/id fh/;

    my $size = sysseek( $fh, 0, SEEK_END );
    sysseek($fh, 0, SEEK_SET );

    my $index = $self->stack_index; #with lock
    my $stack_id = $index->id;
    my $stack_fh = $self->open_stack($stack_id);

    my $offset = sysseek( $stack_fh, 0, SEEK_END ) // die $!;
    $offset += 0;
    if ( $offset + $size > $self->max_file_size ) {
        $offset = 0;
        $stack_id = $index->incr;
        $stack_fh = $self->open_stack($stack_id);
    }
    my $len = syswrite($stack_fh, pack('q',KyotoCabinet::hash_murmur($id)), $OBJECT_HEAD_OFFSET) or die $!;
    die "couldnt write object header" if $len < 8;
    copy( $fh, $stack_fh ) or die $!;

    $self->put_pos({
        id => $id,
        stack => $stack_id,
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
        $id = 1;
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

