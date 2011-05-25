use inc::Module::Install;
name 'SoupStack';
all_from 'lib/SoupStack.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
