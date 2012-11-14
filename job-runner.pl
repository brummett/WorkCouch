#!/usr/bin/perl

use strict;
use warnings;

use lib '/gscuser/abrummet/newworkflow/task-based/lib';
use WorkflowComms;

my $uri = 'http://linus146:5985/workflow';
my $server = WorkflowComms->new($uri);

my $job_id = $ARGV[0];

print STDERR "job runner for job $job_id\n";

my $job = $server->_get_doc($job_id);
my $command = $job->{'cmdline'};

my $pid = fork();
if (defined($pid) && !$pid) {
    # child
    print STDERR "Job runner about to exec $command\n";
    exec($command);
    exit(1);
}

# parent
print STDERR "Job runner child pid is $pid\n";
my $result = $server->job_is_running($job_id, $pid);
if ($result ne 'success') {
    die "Couldn't set job $job_id running: $result";
}

print STDERR "Job runner waiting for child $pid to exit\n";
waitpid($pid, 0);
my($user,$system,$cuser,$csystem) = times();

my $exit_code = $? >> 8;
print STDERR "Job runner child $pid is done! exit code $exit_code\n";
my $signal = $? & 127;
my $coredump = $? & 128;

if ($exit_code) {
    # crashed!
    $server->job_is_crashed($job_id, result => $exit_code, signal => $signal, coredump => $coredump, cpuTime => ($cuser+$csystem));
} else {
    $server->job_is_done($job_id, result => $exit_code);
}


