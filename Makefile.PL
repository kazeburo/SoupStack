use inc::Module::Install;
name 'SoupStack';
all_from 'lib/SoupStack.pm';

# requires '';
requires 'Plack';
requires 'Router::Simple';
requires 'HTTP::Exception';
requires 'Try::Tiny';
requires 'Mouse';
requires 'KyotoCabinet';
requires 'Data::MessagePack';
requires 'Data::Validator';
requires 'File::Copy';
requires 'Cache::LRU';
requires 'IO::File';
requires 'List::MoreUtils';


tests 't/*.t t/*/*.t';
#author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
#auto_include;
WriteAll;
