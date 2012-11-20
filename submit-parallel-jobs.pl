#!/usr/bin/perl

use strict;
use warnings;

use WorkflowComms;

my $count = $ARGV[0] - 2;  # subtract the starting and ending nodes

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

# make a starting node
my $starting_node = {
    workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'start',
        cmdline     => '/bin/sleep 0',
        dependants  => [],
        waitinOn    => 0,
    };


my @middle_nodes;
foreach my $i ( 1 .. $count ) {
    push @middle_nodes, {
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'middle',
        cmdline     => '/bin/sleep 0',
        waitingOn   => 1,
    };
}

# and an ending node
my $ending_id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'end',
        cmdline     => '/bin/sleep 0',
        waitingOn   => scalar(@middle_nodes),
    });

my @middle_ids;
foreach (@middle_nodes) {
    $_->{'dependants'} = [ $ending_id ];
    my $id = $server->enqueue($_);
    push @middle_ids, $id;
}

$starting_node->{'dependants'} = \@middle_ids;
$server->enqueue($starting_node);
