#!/usr/bin/perl

use WorkflowComms;

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

# make a starting node
my $starting_id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => 'sleep 0',
        #depends     => undef
        waitinOn    => 0,
    });

my @middle_ids;
foreach my $i ( 1 .. 98 ) {
    my $id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => 'sleep 0',
        waitingOn   => 1,
    });
    push @middle_ids, $id;
}

$server->add_dependant($starting_id, $_) foreach @middle_ids;

# and an ending node
my $ending_id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => 'sleep 0',
        #waitingOn   => 98,
        waitingOn   => scalar(@middle_ids),
    });

$server->add_dependant($_, $ending_id) foreach @middle_ids;
