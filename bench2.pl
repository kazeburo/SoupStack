use strict;
use Test::More;
use SoupStack::Storage;
use File::Temp qw/tempdir/;
use File::Copy;
use Benchmark qw/timethese/;
use List::Util qw/shuffle/;

open(my $fh,"output");

my $dir = tempdir( CLEANUP => 1 );
warn $dir;
my $storage = SoupStack::Storage->new({
    root => $dir,
    max_file_size => 1_000_000_000,
});

my $i=1;
timethese(20_000,{
    write => sub {
        $storage->put($i, $fh);
        $i++;
    }
});

timethese(20_000,{
    read => sub {
        $storage->get(int(rand(20_000)+1));
    }
});

