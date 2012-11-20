#!/usr/bin/perl

use WorkflowComms;

my $count = $ARGV[0];

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

my $last_doc_id;
foreach my $i ( 1 .. $count ) {
    my $id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => '/bin/sleep 0',
        #cmdline     => '/bin/echo hi',
        waitingOn   => $last_doc_id ? 1 : 0,
    });

    if ($last_doc_id) {
        $server->add_dependant($last_doc_id, $id);
    }
    
    $last_doc_id = $id;
}
