use strict;
use Test::More;
use SoupStack::Storage;
use File::Temp qw/tempdir/;
use File::Copy;
use Benchmark qw/timethese/;
use List::Util qw/shuffle/;

my $try=5000;

open(my $fh, 'output');

my $dir = tempdir( CLEANUP => 1 );
my $storage = SoupStack::Storage->new({
    root => $dir,
    max_file_size => 10_000_000,
});

my $i=0;
my $k=0;
timethese($try,{
    write_soupstack => sub {
        $i++;
        $storage->put($i, $fh);
    },
    write_fileio => sub {
        $k++;
        seek($fh,0,0);
        copy( $fh, "$dir/$k", 65536 );
    },
});

timethese($try,{
    read_soupstack => sub {
        $storage->get( int(rand($try)) );
    },
    read_fileio => sub {
        open(my $fh1, "$dir/".int(rand($try)) );
        binmode($fh1);
        read($fh1, my $buf, 16);
    },
});
