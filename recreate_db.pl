#!/usr/bin/perl

use LWP;

my $uri = 'http://localhost:5985/workflow';

my $server = LWP::UserAgent->new();
my $req = HTTP::Request->new(DELETE => $uri);
my $resp = $server->request($req);


$req = HTTP::Request->new(PUT => $uri);
$resp = $server->request($req);


