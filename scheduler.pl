#!/usr/bin/perl

use strict;
use warnings;

use WorkflowComms;
use IO::Select;


my $uri = 'http://localhost:5985/workflow';
my $server = WorkflowComms->new($uri);

my $last_seq = $ARGV[0] || $server->current_update_seq();

my $changes_fh = $server->changes_for_scheduler($last_seq);
my $changes_select = IO::Select->new($changes_fh);

while(1) {
      
    my $job_id = $server->get_next_runnable_job();
    if ($job_id) {
        $server->schedule_job($job_id, 'fork');
    } else {
        print "Waiting for something to run...\n";
        while ($changes_select->can_read(1)) {
            clean_up_from_done_job($changes_fh);
        }
    }
}

sub clean_up_from_done_job {
    my $fh = shift;
    my $id = $server->get_finished_job_id_from_changes($fh);
    $server->remove_job_as_dependancy($id);
}
