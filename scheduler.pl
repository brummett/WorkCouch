#!/usr/bin/perl

use strict;
use warnings;

use WorkflowComms;
use AnyEvent;
use Data::Dumper;

my $DEBUG = 0;

my $waiting_on_jobs = $ARGV[0] || 100;

my $uri = 'http://localhost:5985/workflow';
my $server = WorkflowComms->new($uri);

my $last_seq = $server->current_update_seq();

my $changes_fh = $server->changes_for_scheduler($last_seq);
my $changes_watcher = AnyEvent->io(fh => $changes_fh, poll => 'r',
                                    cb => sub { message_from_db($changes_fh) });

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
        print "Scheduling job $_\n" if ($DEBUG);
        #$server->schedule_job($_, 'fork');
        $server->schedule_job_fake($_, 'fork');
    }
}

sub message_from_db {
    my $fh = shift;

    my $line = $server->_read_line_from_fh($fh);
        if (!$line) {
        # eof?
        #print "EOF on changes feed?!, exiting";
        $fh->close();
        $changes_watcher->cancel();
        $done->send;
        return;
    }

    my $data = $server->json_decode($line);
#print "Got message from DB: ".Data::Dumper::Dumper($data);

    my $doc = $data->{'doc'};
    if ($doc->{'status'} eq 'done') {
        # a job is finished - decrement its dependants waitingOn
        $server->signalChildren($doc);
        $waiting_on_jobs--;

    } elsif (($doc->{'status'} eq 'waiting') and ($doc->{'waitingOn'} == 0)) {
        # A job is now ready to run
        $server->schedule_job_fake($doc->{'_id'});
    } else {
        die "Unknown doc received from changes: ".Data::Dumper::Dumper($doc);
    }

    if ($waiting_on_jobs == 0) {
        $done->send;
    }
}


#sub clean_up_from_done_job {
#    my $fh = shift;
#    $DB::single=1;
#    my $line = $server->_read_line_from_fh($fh);
#    if (!$line) {
#        # eof?
#        #print "EOF on changes feed?!, exiting";
#        $fh->close();
#        $changes_watcher->cancel();
#        $done->send;
#        return;
#    }
#
#    $waiting_on_jobs--;
#        
#    my $data = $server->json_decode($line);
#    #print "Got data: ",Data::Dumper::Dumper($data);
#
#    $server->remove_job_as_dependancy($data->{id});
#
#    &start_runnable_jobs();
#
#    if ($waiting_on_jobs == 0) {
#        $done->send;
#    }
#}
