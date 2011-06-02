package SoupStack::Storage;

use strict;
use warnings;
use 5.10.0;
use Fcntl qw/:DEFAULT :flock :seek/;
use Cache::LRU;
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

sub lock_index {
    my $self = shift;
    my $id = shift;
    SoupStack::Storage::LockIndex->new( root => $self->root, id => $id, fh_cache => $self->fh_cache );
}

sub stack_index {
    my $self = shift;
    my ( $index, $rid ) = @_;
    my $key = sprintf "stack_%010d_%s", $index, $rid; 
    if ( my $cached = $self->fh_cache->get($key) ) {
        return $cached;
    }
    my $stack_index = SoupStack::Storage::StackIndex->new(
        root => $self->root,
        index => $index,
        rid => $rid
    );
    $self->fh_cache->set($key, $stack_index);
    $stack_index;
}

sub find_stack {
    my ( $self, $id ) = @_;

    my $path = $self->root ."/stack.index";
    return unless -f $path;

    if ( my $cached = $self->fh_cache->get('stack.index') ) {
        my @stat = stat _; #path
        if ( $cached->[0] == $stat[10] ) {
            foreach my $index ( reverse @{$cached->[1]} ) {
                if ( $id >= $index->[1] ) {
                    return $index->[0];
                }
            }
        }
    }

    sysopen( my $fh, $path, O_RDONLY ) or die "Couldnt open lockfile: $!";
    my @index;
    my $end = sysseek($fh, 0, SEEK_END);
    sysseek($fh, 0, SEEK_SET);
    for ( my $index=1; $index <= $end / 31; $index++ ) {
        my $rlen = sysread( $fh, my $buf, 31 );
        die "unexpected eof while reading index: $!" if $rlen != 31;
        my $index = int(substr($buf, 0, 10));
        my $head_id = int(substr($buf, 10, 20));
        push @index, [$index,$head_id];
    }

    my @stat = stat $path;
    $self->fh_cache->set('stack.index', [$stat[10],\@index]);

    foreach my $index ( reverse @index ) {
        if ( $id >= $index->[1] ) {
            return $index->[0];
        }
    }

    return;
}

sub open_stack {
    my ($self,$index, $create_with_id, $without_setcache) = @_;

    my $key = sprintf "stack_%010d", $index;
    my $path = $self->root . '/' . $key;
    my $cached = $self->fh_cache->get($key);
    if ( $cached ) {
        my @stat = stat $path;
        return $cached if $cached->{ctime} == $stat[10];
    }

    my $fh;
    my $rid;
    my $head_id;
    if ( $create_with_id && ! -f $path ) {
        sysopen( $fh, $path, O_RDWR|O_CREAT ) or die $!;
        $rid = time;
        syswrite($fh, pack('L',$rid), 4) or die $!; #time
        $head_id = $create_with_id;
    }
    else {
        sysopen( $fh, $path, O_RDWR ) or die $!;
        sysread( $fh, my $buf, 12) // die $!; #time+first object id
        ($rid,$head_id) = unpack('LQ>',$buf);
    }

    my @stat = stat $path;
    my $stack = { rid => $rid, fh => $fh, ctime => $stat[10], head_id => $head_id  };
    $self->fh_cache->set( $key, $stack) if !$without_setcache;
    $stack;
}

sub get {
    my ($self,$id) = @_;

    my $index = $self->find_stack($id);
    return unless $index;
    my $stack = $self->open_stack($index);
    my $pos = $self->stack_index(
        $index,
        $stack->{rid}
    )->search($id);
    return unless $pos;

    return if $pos->{deleted};
    sysseek( $stack->{fh}, $pos->{offset},  SEEK_SET ) or die $!;
    my $rlen = sysread( $stack->{fh}, my $buf, 16 );
    die "unexpected eof while reading object: $!" if $rlen != 16;
    my ($object_id, $size) = unpack('Q>Q>', $buf);
    die "unexpected object_id, $id, $object_id, $pos->{id}, $pos->{offset}" if $object_id != $id;

    return SoupStack::Storage::RangeFile->new(
        $stack->{fh}, 
        $size,
        $pos->{offset}+16,
    );
}

sub delete {
    my ($self,$id) = @_;
    my $index = $self->find_stack($id);
    return unless $index;
    my $stack = $self->open_stack($index);
    $self->stack_index(
        $index,
        $stack->{rid}
    )->delete($id);
    return 1;
}

sub put {
    my $self = shift;
    my ($id,$fh) = @_;

    my $size = sysseek( $fh, 0, SEEK_END );
    sysseek($fh, 0, SEEK_SET );

    die 'cannot store size > max_file_size' 
        if $size + 16 >= $self->max_file_size;

    my $lock_index = $self->lock_index($id); #with lock
    my $index = $lock_index->latest_id;
    my $stack = $self->open_stack($index, $id);

    my $offset = sysseek( $stack->{fh}, 0, SEEK_END ) // die $!;
    $offset += 0;

    if ( $offset + $size > $self->max_file_size ) {
        $index = $lock_index->incr();
        $stack = $self->open_stack($index,$id);
        $offset = sysseek( $stack->{fh}, 0, SEEK_END ) // die $!;
        $offset += 0;
    }

    my $len = syswrite($stack->{fh}, pack('Q>Q>',$id,$size), 16) or die $!;
    die "couldnt write object header" if $len < 16;

    for (;;) {
        my ($readed, $wrote, $pwrote);
        defined($readed = sysread($fh, my $buf, 65536)) or die "unexpected eof: $!";
        last unless $readed;
        for ($wrote = 0; $wrote < $readed; $wrote += $pwrote) {
            $pwrote = syswrite($stack->{fh}, $buf, $readed - $wrote, $wrote) or die "cannot write to stack: $!";
        }
    }

    my $stack_index = $self->stack_index($index,$stack->{rid});
    $stack_index->add($id, $offset);
    1;
}

1;

package SoupStack::Storage::StackIndex;

use strict;
use warnings;
use Fcntl qw/:DEFAULT :flock :seek/;
use Cache::LRU;

my $READAHEAD=4096;

sub new {
    my $class = shift;
    my %args = ref $_[0] ? %{$_[0]} : @_;
    my $self = bless \%args, $class;

    my $key = sprintf "stack_%010d_%s.index", $self->{index}, $self->{rid};
    my $path = $self->{root} . '/' . $key;
    sysopen( my $fh, $path, O_RDWR|O_CREAT ) or die $!;
    binmode($fh);
    $self->{fh} = $fh;
    $self->{cache} = Cache::LRU->new(size=>200);
    $self;
}

sub add {
    my $self = shift;
    my ($id, $offset) = @_;
    my $fh = $self->{fh};
    my $end = sysseek($fh, 0, SEEK_END);
    if ( $end > 0 ) {
        sysseek($fh, $end - 17, SEEK_SET);
        sysread($fh, my $buf, 8);
        my $last_id = unpack('Q>', $buf);
        die "id order [$id], current id is $last_id" if $last_id >= $id;
        sysseek($fh, 0, SEEK_END);
    }
    my $write = syswrite($fh, pack('Q>Q>a',$id,$offset,0), 17);
    die "index write error: $!" if $write < 17;
}

sub membinsearch {
    my ($find, $buf, $cur, $end ) = @_;
    return if $end - $cur < 17;
    my $pos = int(($cur + $end ) / 2);
    $pos = $pos - $pos % 17;
    my $id = substr($$buf, $pos, 8);
    if ( $find eq $id ) {
        return [unpack('Q>Q>a',substr($$buf, $pos, 17)), $pos];
    }
    elsif ( $find gt $id ) {
        membinsearch( $find, $buf, $pos, $end);
    }
    elsif ( $find le $id ) {
        membinsearch( $find, $buf, $cur, $pos);
    }    
}

sub readahead {
    my ($fh, $pos, $len) = @_;
    if ( $len > $READAHEAD ) {
        sysseek( $fh, $pos, SEEK_SET);
        my $readed = sysread($fh, my $buf, $len);
        die "unexpected eof in readahead: $!" if $readed < $len;
        return $buf;
    }
    my $readpos = $pos - ($pos % $READAHEAD);
    if ( $pos + $len > $readpos + $READAHEAD ) {
        sysseek( $fh, $readpos, SEEK_SET);
        my $readed = sysread($fh, my $buf, $READAHEAD*2 );
        die "unexpected eof in readahead: $!" if $readed < $pos + $len - $readpos;
        return substr( $buf, $pos - $readpos, $len);
    }

    sysseek($fh, $readpos, SEEK_SET);
    my $readed = sysread($fh, my $buf, $READAHEAD);
    die "unexpected eof in readahead: $!" if $readed < $pos + $len - $readpos;
    substr( $buf, $pos - $readpos, $len);
}

sub binsearch {
    my ($find, $fh, $cur, $end) = @_;
    return if $end - $cur < 17;
    if ( $end - $cur <= 16384) {
        my $end_pos = $end - $cur;
        $end_pos = $end_pos - $end_pos % 17;
        my $buf = readahead( $fh, $cur, $end_pos);
        my $ret =  membinsearch($find, \$buf, 0, $end_pos);
        if ( $ret ) {
            return [$ret->[0],$ret->[1],$ret->[2],$ret->[3]+$cur];
        }
        return;
    }
    my $pos = int(($cur + $end ) / 2);
    $pos = $pos - $pos % 17;
    my $buffer = readahead( $fh, $pos, 17);    
    my $id = substr( $buffer, 0, 8);
    if ( $find eq $id ) {
        return [unpack('Q>Q>a',$buffer),$pos];
    }
    elsif ( $find gt $id ) {
        binsearch( $find, $fh, $pos, $end);
    }
    elsif ( $find le $id ) {
        binsearch( $find, $fh, $cur, $pos);
    }
}

sub search {
    my $self = shift;
    my $id = shift;
    my $end = sysseek($self->{fh}, 0, SEEK_END );
    return unless $end;
    my $ret = binsearch(pack('Q>',$id), $self->{fh}, 0, $end);
    return unless $ret;
    return {
        id => $ret->[0],
        offset => $ret->[1],
        deleted => $ret->[2],
        pos => $ret->[3],
    }
}

sub delete {
    my $self = shift;
    my $id = shift;
    my $search = $self->search($id);
    return unless $search;

    sysseek($self->{fh}, $search->{pos}, SEEK_SET);
    syswrite($self->{fh}, pack('Q>Q>a',$search->{id},$search->{offset},1), 17);

    my $path = sprintf "%s/stack_%010d.deleted",$self->{root}, $self->{index};
    sysopen( my $fh, $path, O_RDWR|O_CREAT ) or die $!;
    flock( $fh, LOCK_EX ) or die "Couldnt get lock: $!";
    sysseek($fh, 0, SEEK_END);
    syswrite($fh, pack('Q>', $id), 8);
}

1;

package SoupStack::Storage::LockIndex;

use strict;
use warnings;
use Fcntl qw/:DEFAULT :flock :seek/;
use File::Copy;

sub new {
    my $class = shift;
    my %args = ref $_[0] ? %{$_[0]} : @_;
    my $self = bless \%args, $class;

    my $path = $self->{root} ."/.stack.index";
    my $fh;
    if ( !( $fh = $self->{fh_cache}->get('.stack.index')) ) {
        sysopen( $fh, $path, O_RDWR|O_CREAT ) or die "Couldnt open lockfile: $!";
        $self->{fh_cache}->set('.stack.index',$fh);
    }

    flock( $fh, LOCK_EX ) or die "Couldnt get lock: $!";
    $self->{_fh} = $fh;

    my $end = sysseek($fh, 0, SEEK_END);
    if ( $end == 0 ) {
        sysseek( $fh, 0, SEEK_END) or die $!;
        syswrite( $fh, sprintf("%10d%20d\n",1,$self->{id}), 31 ) or die $!;
        $self->{modify} = 1;
        $end = 31;
    }
    $self->{latest_id} = $end /31;

    return $self;
}

sub latest_id {
    shift->{latest_id};
}

sub incr {
    my $self = shift;
    my $last_id = $self->latest_id;
    $last_id++;
    sysseek( $self->{_fh}, 0, SEEK_END) or die $!;
    syswrite( $self->{_fh}, sprintf("%10d%20d\n",$last_id,$self->{id}), 31 ) or die $!;
    $self->{modify} = 1;
    return $last_id;
}

sub DESTROY {
    my $self = shift;
    return if !$self->{_fh};
    flock( $self->{_fh}, LOCK_UN ) or die "unlockerror: $!";
    copy($self->{root} ."/.stack.index", $self->{root} . "/stack.index") if $self->{modify};
}

1;

package SoupStack::Storage::RangeFile;

use strict;
use warnings;
use 5.10.0;
use Fcntl qw/:seek/;

sub new {
    my ($class, $fh, $count, $offset) = @_;
    my $self = bless {
        fh => $fh,
        _count => $count,
        _offset => $offset,
    }, $class;

    if ( ! defined $offset ) {
        seek( $fh, 0, SEEK_CUR) or die $!;
        my $cur = tell($fh);
        die $! if $cur < 0;
        $self->{_offset} = $cur;
    }
    if ( ! $count ) {
        seek( $fh, 0, SEEK_END) or die $!;
        my $end = tell($fh);
        die $! if $end < -1;
        $self->{_count} = $end - $offset;
    }
    seek( $fh, $self->{_offset}, SEEK_SET) or die $!;

    $self;
}

sub getline {
    my $self = shift;
    return if defined $self->{_read} && $self->{_read} >= $self->{_count};
    $self->{_read} = 0 if ! exists $self->{_read};

    my $len = ref $/ ? ${$/} : 65536;
    $len = $self->{_count} - $self->{_read} if $len > $self->{_count} - $self->{_read};
    my $read = read( $self->{fh}, my $buf, $len);
    die $! if ! defined $read;

    $self->{_read} += $read;
    $self->{_offset} = $self->{_offset} + $self->{_read};
    return $buf;
}


sub close { 1 };

1;


