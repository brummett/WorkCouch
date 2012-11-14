#!/usr/bin/perl

use strict;
use warnings;

use WorkflowComms;
use AnyEvent;
use Data::Dumper;

my $uri = 'http://localhost:5985/workflow';
my $server = WorkflowComms->new($uri);

my $last_seq = $ARGV[0] || $server->current_update_seq();

my $changes_fh = $server->changes_for_scheduler($last_seq);
my $changes_watcher = AnyEvent->io(fh => $changes_fh, poll => 'r',
                                    cb => sub { clean_up_from_done_job($changes_fh) });

my $done = AnyEvent->condvar;
my $int_watcher = AnyEvent->signal(signal => 'INT', cb => sub{ $done->send });

# Initially, get the list of runnable jobs and run them
&start_runnable_jobs();

# Now, enter the event loop.  Further processing will be initiated from
# the changes feed
$done->recv();

sub start_runnable_jobs {
    my $ready_job_ids = $server->get_runnable_jobs();
    foreach ( @$ready_job_ids) {
        $server->schedule_job($_, 'lsf');
    }
}

sub clean_up_from_done_job {
    my $fh = shift;
    $DB::single=1;
    my $line = $server->_read_line_from_fh($fh);
    if (!$line) {
        # eof?
        print "EOF on changes feed?!, exiting";
        $fh->close();
        $changes_watcher->cancel();
        $done->send;
    }
        
    my $data = $server->json_decode($line);
    print "Got data: ",Data::Dumper::Dumper($data);

    $server->remove_job_as_dependancy($data->{id});

    &start_runnable_jobs();
}
