package SoupStack::Storage;

use strict;
use warnings;
use 5.10.0;
use Mouse;
use Scope::Container::DBI;
use DBI qw(:sql_types);
use Data::MessagePack;
use Data::Validator;
use Fcntl qw/:DEFAULT :flock :seek/;
use File::Copy;
use Cache::LRU;
use SoupStack::Constraints;
use Plack::TempBuffer;

has 'root' => (
    is => 'ro',
    isa => 'RootDir',
    coerce => 1,
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

my $_on_connect = sub {
    my $connect = shift;
    $connect->do(<<EOF);
CREATE TABLE IF NOT EXISTS stack (
    id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    pos BLOB NOT NULL
)
EOF
    return;
};

sub db {
    my $self = shift;
    my $path = $self->root->file("stack.db");
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    Scope::Container::DBI->connect("dbi:SQLite:dbname=$path",'','', {
        Callbacks => {
            connected => $_on_connect,
        },
    });
}

sub find_pos {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
    );
    my $args = $rule->validate(@_);
    my $pos = $self->db->select_one('SELECT pos FROM stack WHERE id=?', $args->{id});
    return unless $pos;
    Data::MessagePack->unpack($pos);
}

sub put_pos {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
        stack => 'Int',
        offset => 'Int',
        size => 'Int',
    );
    my $args = $rule->validate(@_);

    my $pos = Data::MessagePack->pack({
        stack => $args->{stack},
        offset => $args->{offset},
        size => $args->{size},
    });
    
    my $sth = $self->db->prepare(q{INSERT OR REPLACE INTO stack (id, pos) VALUES ( ?, ?)});
    $sth->bind_param(1, $args->{id}, SQL_INTEGER);
    $sth->bind_param(2, $pos, SQL_BLOB);
    $sth->execute;
}

sub delete_pos {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
    );
    my $args = $rule->validate(@_);
    $self->db->select_one('DELETE FROM stack WHERE id=?', $args->{id});
}

sub stack_index {
    my $self = shift;
    SoupStack::Storage::StackIndex->new( root => $self->root );
}

sub open_stack {
    my ($self,$index) = @_;
    my $fh_cache = $self->fh_cache->get($index);
    return $fh_cache if $fh_cache;
    sysopen( my $fh, $self->root->file(sprintf("stack_%010d",$index)), O_RDWR|O_CREAT ) or die $!;
    $self->fh_cache->set( $index, $fh);
    $fh;
}

sub get {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
    );
    my $args = $rule->validate(@_);
    my $id = $args->{id};

    my $pos = $self->find_pos( id => $id );
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
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
    );
    my $args = $rule->validate(@_);
    $self->delete_pos( id => $args->{id} );
    return 1;
}

sub put {
    my $self = shift;
    state $rule = Data::Validator->new(
        id => 'Int',
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
    my $len = syswrite($stack_fh, pack('Q',$id), 8) or die $!;
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
use SoupStack::Constraints;

has 'root' => (
    is => 'ro',
    isa => 'RootDir',
    coerce => 1,
    required => 1,
);

sub BUILD {
    my $self = shift;
    my $path = $self->root->file("stack.index");
    sysopen( my $fh, $path, O_RDWR|O_CREAT ) or die "Couldnt open lockfile: $!";
    flock( $fh, LOCK_EX ) or die "Couldnt get lock: $!";
    $self->{_fh} = $fh;

    sysseek( $fh, 0, SEEK_SET) or die $!;
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

