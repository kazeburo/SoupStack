use strict;
use Test::More;
use SoupStack::Storage;
use File::Temp qw/tempdir/;
use File::Copy;
use Benchmark qw/timethese/;
use List::Util qw/shuffle/;

open(my $fh, 'output');

my $dir = tempdir( CLEANUP => 1 );
my $storage = SoupStack::Storage->new({
    root => $dir,
    max_file_size => 10_000_000,
});

for(1..5000) {
    $storage->put($_, $fh);
}

for(1..5000) {
    $storage->get($_);
}



