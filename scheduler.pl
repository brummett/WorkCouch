#!/usr/bin/perl

use strict;
use warnings;

use WorkflowComms;
use AnyEvent;
use Data::Dumper;

our $DEBUG = 0;

my $waiting_on_jobs = $ARGV[0] || 100;

my $uri = 'http://localhost:5985/workflow';
my $server = WorkflowComms->new($uri);

my $last_seq = $server->current_update_seq();

my $changes_fh = $server->changes_for_scheduler($last_seq);
my $changes_watcher = AnyEvent->io(fh => $changes_fh, poll => 'r',
                                    cb => sub { message_from_db($changes_fh) });

my @children_to_signal;
my $child_signal_watcher = AnyEvent->idle(cb => sub {
        if (@children_to_signal) {
            $server->signalChildJob(shift @children_to_signal);
        }
});

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
        $server->schedule_job($_, 'null');
    }
}

sub message_from_db {
    my $fh = shift;

    print "Reading from changes feed...\n" if ($DEBUG);
    my @lines = split(/\n/, $server->read_all_lines_from_fh($fh));
    foreach my $line ( @lines ) {

        my $data = eval { $server->json_decode($line) };
        if ($@) {
            die "Couldn't parse json message: >>>$line<<<\n";
        }
        print "Got message from DB: ".Data::Dumper::Dumper($data) if ($DEBUG);

        my $doc = $data->{'doc'};
        if ($doc->{'status'} eq 'done') {
            # a job is finished - decrement its dependants waitingOn
            if ($doc->{'dependants'}) {
                print "Telling ".scalar(@{$doc->{'dependants'}})." child jobs to dec counter\n" if ($DEBUG);
                push @children_to_signal, @{$doc->{'dependants'}};
            }
            $waiting_on_jobs--;

        } elsif (($doc->{'status'} eq 'waiting') and ($doc->{'waitingOn'} == 0)) {
            # A job is now ready to run
            print "Scheduling job ".$doc->{_id}."\n" if ($DEBUG);
            #$server->schedule_job($doc->{'_id'}, 'fork');
            $server->schedule_job($doc->{'_id'}, 'null');
        } else {
            die "Unknown doc received from changes: ".Data::Dumper::Dumper($doc);
        }

        if ($waiting_on_jobs == 0) {
            $done->send;
        }
    }
}

