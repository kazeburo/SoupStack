use strict;
use Test::More;
use SoupStack::Storage;
use File::Temp qw/tempdir/;
use List::Util qw/shuffle/;

my $dir = tempdir( CLEANUP => 1 );

my $storage = SoupStack::Storage->new({
    root => $dir,
    max_file_size => 1_000_000,
});
ok($storage);

open(my $fh, __FILE__);
my $file = do { local $/; <$fh> };

for my $id (1..20){    
    ok($storage->put($id, $fh));
    my $fh1 = $storage->get($id);
    ok($fh1);
    my $file1 = do { local $/; <$fh1> };
    is($file, $file1);
}

for my $id (shuffle 1..20){
    my $fh1 = $storage->get($id);
    ok($fh1);
}

for my $id (1..20) {
    ok($storage->delete($id));
    my $fh1 = $storage->get($id);
    ok(!$fh1)
}


done_testing;



