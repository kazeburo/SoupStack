use inc::Module::Install;
name 'SoupStack';
all_from 'lib/SoupStack.pm';

# requires '';
requires 'Plack';
requires 'Router::Simple';
requires 'HTTP::Exception';
requires 'Try::Tiny';
requires 'Mouse';
requires 'File::Copy';
requires 'Cache::LRU';

tests 't/*.t t/*/*.t';
#author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
#auto_include;
WriteAll;
