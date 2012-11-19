#!/usr/bin/perl

use WorkflowComms;

my $count = $ARGV[0] - 2;  # subtract the starting and ending nodes

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

# make a starting node
#my $starting_id = $server->enqueue({
#        workflowId  => 1,
#        clusterId   => 1,
#        queueId     => 'short',
#        label       => 'sleep',
#        cmdline     => '/bin/sleep 0',
#        #depends     => undef
#        waitinOn    => 0,
#    });

my $starting_node = {
    workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => '/bin/sleep 0',
        dependants  => [],
        waitinOn    => 0,
    };


#my @middle_ids;
my @middle_nodes;
foreach my $i ( 1 .. $count ) {
#    my $id = $server->enqueue({
#        workflowId  => 1,
#        clusterId   => 1,
#        queueId     => 'short',
#        label       => 'sleep',
#        cmdline     => '/bin/sleep 0',
#        waitingOn   => 1,
#    });
#    push @middle_ids, $id;
    push @middle_nodes, {
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => '/bin/sleep 0',
        waitingOn   => 1,
    };
}

#$server->add_dependant($starting_id, $_) foreach @middle_ids;

# and an ending node
my $ending_id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => '/bin/sleep 0',
        waitingOn   => scalar(@middle_ids),
    });
#$server->add_dependant($_, $ending_id) foreach @middle_ids;

my @middle_ids;
foreach (@middle_nodes) {
    $_->{'dependants'} = [ $ending_id ];
    my $id = $server->enqueue($_);
    push @middle_ids, $id;
}

$stating_node->{'dependants'} = \@middle_ids;
$server->enqueue($starting_node);
