#!/usr/bin/perl

use WorkflowComms;

my $server = WorkflowComms->new('http://localhost:5985/workflow/');

my $last_doc_id;
foreach my $i ( 1 .. 100 ) {
    my $id = $server->enqueue({
        workflowId  => 1,
        clusterId   => 1,
        queueId     => 'short',
        label       => 'sleep',
        cmdline     => 'sleep 0',
        depends     => $last_doc_id ? [$last_doc_id] : undef,
    });
    
    $last_doc_id = $id;
}
