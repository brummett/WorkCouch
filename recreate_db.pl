#!/usr/bin/perl

use strict;
use warnings;

use LWP;
use JSON;

my %dbs = ( 'http://linus146:5985' => 'workflow',
            'http://linus222:5985'  => 'workflow',
          );


foreach my $db ( keys %dbs ) {
    my $uri = join('/', $db, $dbs{$db});

    my $server = LWP::UserAgent->new();

    print "Deleting DB $uri\n";
    my $req = HTTP::Request->new(DELETE => $uri);
    my $resp = $server->request($req);

    print "Creating DB $uri\n";
    $req = HTTP::Request->new(PUT => $uri);
    $resp = $server->request($req);

    sleep 1;

    print "Pushing design doc to $uri\n";
    `NODE_PATH=~/lib/node_modules/ couchapp push workflow-scheduler.js $uri`;
}

# Set up pull replication between DBs
#
# This uses the OLD _replicate API, since Ubuntu's couchDB is 1.0.1
# see http://wiki.apache.org/couchdb/Replication
my $json = JSON->new();
foreach my $local_db ( keys %dbs ) {
    my $local_uri = join('/', $local_db, $dbs{$local_db});
    
    my $server = LWP::UserAgent->new();

    foreach my $remote_db ( keys %dbs ) {
        my $remote_uri = join('/', $remote_db, $dbs{$remote_db});
        next if ($local_uri eq $remote_uri);

        my $doc = {
            source => $remote_uri,
            target => 'workflow',
            continuous => JSON::true,
        };
        my $req = HTTP::Request->new(POST => $local_db . '/_replicate');
        $req->header('Content-Type', 'application/json');
        $req->content($json->encode($doc));
        print "Replicating from $remote_uri to $local_uri\n";
        my $resp = $server->request($req);
        die "Replication didn't work! from $remote_uri to $local_uri" unless ($resp->is_success);
    }
}
