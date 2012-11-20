#!/usr/bin/perl

use strict;
use warnings;

use JSON;
use WorkflowComms;

my $count = $ARGV[0] - 2;  # subtract the starting and ending nodes

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

my $starting_node = {
    workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'start',
        cmdline     => '/bin/sleep 0',
        dependants  => [],
        waitinOn    => 0,
    };

my @jobs = map { '/bin/sleep 0' } ( 1 .. $count );
my $parallel_node = {
    workflowId  => 1,
    clusterId   => 1,
    queueId     => 'short',
    label       => 'parallel',
    isParallel  => JSON::true,
    cmdline     => \@jobs,
    dependants  => [],
    waitingOn   => 1,
};


# and an ending node
my $ending_id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'end',
        cmdline     => '/bin/sleep 0',
        waitingOn   => 1,
    });

$parallel_node->{'dependants'} = [ $ending_id ];
my $parallel_id = $server->enqueue($parallel_node);

$starting_node->{'dependants'} = [ $parallel_id ];
$server->enqueue($starting_node);
