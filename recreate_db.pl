#!/usr/bin/perl

use LWP;

my @uri = ( 'http://localhost:5985/workflow',
            'http://linus222:5985/workflow',
          );


foreach ( @uri ) {
    my $server = LWP::UserAgent->new();
    my $req = HTTP::Request->new(DELETE => $_);
    my $resp = $server->request($req);

    $req = HTTP::Request->new(PUT => $_);
    $resp = $server->request($req);
}


