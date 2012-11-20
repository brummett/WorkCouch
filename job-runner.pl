#!/usr/bin/perl

use strict;
use warnings;

use lib '/gscuser/abrummet/newworkflow/task-based/lib';
use WorkflowComms;
use Time::HiRes;

my $uri = 'http://linus146:5985/workflow';
my $server = WorkflowComms->new($uri);

my $job_id = $ARGV[0];
my $i = $ARGV[1];

#print STDERR "job runner for job $job_id\n";

my $job = $server->_get_doc($job_id);
my $command = $job->{'cmdline'};
if (defined $i) {
    $command = $job->{'cmdline'}->[$i];
}

my $job_start_time = Time::HiRes::time();
my $pid = fork();
if (defined($pid) && !$pid) {
    # child
    #print STDERR "Job runner about to exec $command\n";
    exec($command);
    exit(1);
}

# parent
#print STDERR "Job runner child pid is $pid\n";

my $result = $server->job_is_running($job_id, $i, $pid);
if ($result ne 'success') {
    die "Couldn't set job $job_id running: $result";
}

#print STDERR "Job runner waiting for child $pid to exit\n";
waitpid($pid, 0);
#print STDERR "Job finished: $?\n";

my $job_wall_time = Time::HiRes::time() - $job_start_time;
my($user,$system,$cuser,$csystem) = times();

my $exit_code = ($? >> 8);
#print STDERR "Job runner child $pid is done! exit code $exit_code\n";
my $signal = ($? & 127);
my $coredump = ($? & 128);

my %stats = (   result => $exit_code,
                cpuTime => ($cuser+$csystem),
                wallTime => $job_wall_time );
#print "Job stats: ".Data::Dumper::Dumper(\%stats);

if ($exit_code) {
    # crashed!
#print STDERR "Jobs crashed!\n";
    $server->job_is_crashed($job_id, $i, %stats, signal => $signal, coredump => $coredump);
} else {
#print STDERR "Job is done!\n";
    $server->job_is_done($job_id, $i, %stats);
}


