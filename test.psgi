
use SoupStack;
use File::Temp qw/tempdir/;
use Plack::Builder;

my $dir = tempdir( CLEANUP => 1 );
warn $dir;

my $soupstack = SoupStack->new(
    root => $dir,
);

builder {
    $soupstack->app;
};

