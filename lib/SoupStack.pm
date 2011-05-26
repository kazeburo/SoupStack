package SoupStack;

use strict;
use warnings;
use 5.10.0;
use Plack::Builder;
use Router::Simple;
use HTTP::Exception;
use Try::Tiny;
use SoupStack::Storage;
use Mouse;

our $VERSION = '0.01';

has 'root' => (
    is => 'ro',
    isa => 'Str',
    coerce => 1,
);

has 'max_file_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1_000_000_000 
);

has 'storage' => (
    is => 'ro',
    isa => 'SoupStack::Storage',
    lazy_build => 1,
);

sub _build_model {
    my $self = shift;
    SoupStack::Storage->new(
        root => $self->root,
        max_file_size => $self->max_file_size
    );
}

__PACKAGE__->meta->make_immutable();

sub get_object {
    my ($self,$c) = @_;
    my $id = $c->args->{id};
    my $fh = $c->storage->get($id);
    HTTP::Exception->throw(404) unless $fh;
    $c->res->body($fh);
    $c->res;
}

sub put_object {
    my ($self,$c) = @_;
    my $id = $c->args->{id};
    my $fh = $c->storage->put(id=>$id,fh=>$c->req->body);
    $c->res->body('OK');
    $c->res;
}

sub delete_object {
    my ($self,$c) = @_;
    my $id = $c->args->{id};
    my $fh = $c->storage->get($id);
    $c->res->body('OK');
    $c->res;    
}

sub build_app {
    my $self = shift;

    #router
    my $router = Router::Simple->new;
    $router->connect(
        '/{id:[a-zA-Z0-9/_\-%]+}',
        { action => 'get_object' },
        { method => ['GET','HEAD'] }
    );
    $router->connect(
        '/{id:[a-zA-Z0-9/_\-%]+}',
        { action => 'put_object' },
        { method => ['PUT'] }
    );
    $router->connect(
        '/{id:[a-zA-Z0-9/_\-%]+}',
        { action => 'delete_object' },
        { method => ['DELETE'] }
    );

    sub {
        my $env = shift;

        my $c = SoupStack::Connection->new({
            req => Plack::Request->new($env),
            res => Plack::Response->new(200),
            stash => {},
        });
        my $p = try {
            local $env->{PATH_INFO} = Encode::decode_utf8( $env->{PATH_INFO}, 1 );
            $router->match($env)
        }
        catch {
            HTTP::Exception->throw(400, $_);
        };

        my $response;
        if ( $p ) {
            my $action = delete $p->{action};
            my $code = $self->can($action);
            HTTP::Exception->throw(500, 'Action not Found') unless $code;

            $c->args($p);
            my $res = $code->($self, $c );
            HTTP::Exception->throw(500, 'Undefined Response') if ! $res;

            my $res_t = ref($res) || '';
            if ( blessed $res && $res->isa('Plack::Response') ) {
                $response = $res->finalize;
            }
            elsif ( $res_t eq 'ARRAY' ) {
                $response = $res;
            }
            elsif ( !$res_t ) {
                $c->res->body($res);
                $response = $c->res->finalize;
            }
            else {
                HTTP::Exception->throw("Unknown Response");
            }
        }
        else {
            HTTP::Exception->throw(404, $_);
        }
        $response;
    };
}

sub app {
    my $self = shift;
    my $app = $self->build_app;
    $app = builder {
        enable 'Scope::Container';
        enable 'HTTPExceptions';
        $app;
    };
    $app;
}

package SoupStack::Connection;

use strict;
use warnings;
use Class::Accessor::Lite (
    new => 1,
    rw => [qw/req res stash args/]
);

*request = \&req;
*response = \&res;


1;
__END__

=head1 NAME

SoupStack -

=head1 SYNOPSIS

  use SoupStack;

=head1 DESCRIPTION

SoupStack is

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo {at} gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
