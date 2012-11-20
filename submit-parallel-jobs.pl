#!/usr/bin/perl

use strict;
use warnings;

use JSON;
use WorkflowComms;

my $parallel_max = 25;

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

my @middle_nodes;
while(@jobs) {
    my @par_jobs = splice(@jobs, 0,$parallel_max);

    my $parallel_node = {
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'parallel',
        isParallel  => JSON::true,
        cmdline     => \@par_jobs,
        dependants  => [],
        waitingOn   => 1,
    };
    push @middle_nodes, $parallel_node;
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
# submid the middle nodes
foreach (@middle_nodes) {
    $_->{'dependants'} = [ $ending_id ];
    my $parallel_id = $server->enqueue($_);
    push @middle_ids, $parallel_id;
}

$starting_node->{'dependants'} = \@middle_ids;
$server->enqueue($starting_node);
