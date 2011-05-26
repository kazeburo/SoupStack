package SoupStack::Constraints;

use strict;
use warnings;
use 5.10.0;
use Path::Class::Dir;
use Mouse;
use Mouse::Util::TypeConstraints;

subtype 'RootDir' => as class_type('Path::Class::Dir');
coerce 'RootDir'
    => from 'Str'
        =>  via { Path::Class::Dir->new($_) };

__PACKAGE__->meta->make_immutable();

1;

