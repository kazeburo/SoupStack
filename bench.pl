use strict;
use Test::More;
use SoupStack::Storage;
use File::Temp qw/tempdir/;
use File::Copy;
use Benchmark qw/timethese/;
use Digest::MurmurHash qw/murmur_hash/;
use List::Util qw/shuffle/;

open(my $fh, __FILE__);
my $file = do { local $/; <$fh> };
seek($fh, 0, 0);
warn length($file);


timethese(5,{
    soupstack => sub {
        my $dir = tempdir( CLEANUP => 1 );
        my $storage = SoupStack::Storage->new({
            root => $dir,
            max_file_size => 10_000_000,
        });
        for my $id ( shuffle 1..2_000 ) {
            $storage->put(id=>$id, fh=>$fh);
        }
    },
    fileio => sub {
        my $dir = tempdir( CLEANUP => 1 );
        for my $id ( shuffle 1..2_000 ) {
            copy( $fh, "$dir/".murmur_hash($id));
            seek($fh, 0, 0);
        }
    }
});

my $dir = tempdir( CLEANUP => 1 );
my $storage = SoupStack::Storage->new({
    root => $dir,
    max_file_size => 10_000_000,
});
for my $id (1..2_000 ) {
    $storage->put(id=>$id, fh=>$fh);
}

for my $id (1..2_000 ) {
    copy( $fh, "$dir/".murmur_hash($id));
    seek($fh, 0, 0);
}

timethese(8,{
    read_soupstack => sub {
        my $storage = SoupStack::Storage->new({
            root => $dir,
            max_file_size => 10_000_000,
        });
        for my $id ( shuffle 1..2_000 ) {
            $storage->get($id);
        }
    },
    read_fileio => sub {
        for my $id ( shuffle 1..2_000 ) {
            open(my $fh1, "$dir/".murmur_hash($id) );
            my $result = do { local $/; <$fh1> };
        }
    }
});
