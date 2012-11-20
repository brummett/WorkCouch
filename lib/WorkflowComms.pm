package WorkflowComms;

use strict;
use warnings;

use LWP;
use URI;
use JSON;
use URI::Escape;
use IO::Socket;
use IO::Select;

my $designDoc = '_design/workflow-scheduler';
my $job_runner = '/gscuser/abrummet/newworkflow/task-based/job-runner.pl';

sub new {
    my $class = shift;
    my $uri = shift;

    my $self = bless {}, $class;
    $self->{'json'} = JSON->new->utf8;
    $self->{'server'} = LWP::UserAgent->new();
    $self->{'uri'} = $uri;

    my $uriobj = URI->new($uri);
    $self->{'host'} = $uriobj->host_port;

    return $self;
}

sub _get_doc {
    my($self, $id) = @_;

    my $uri = join('/', $self->{'uri'}, $id);
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);
    return eval { $self->json_decode($resp->content) };
}

sub enqueue {
    my($self, $job) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'enqueue');
    my $req = HTTP::Request->new(POST => $uri);
    $req->header('Content-Type', 'application/json');
    $req->content($self->{'json'}->encode($job));

    my $resp = $self->{'server'}->request($req);

    if ($resp->content =~ m/success: (\w+)/) {
        return $1;
    } else {
        return undef;
    }
}

sub get_runnable_jobs {
    my $self = shift;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_view', 'runnable');
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);

    my $data = $self->json_decode($resp->content);
    return [] unless ($data && $data->{rows});

    return [ map { $_->{id} } @{$data->{rows}} ];
}

sub schedule_job {
    my($self, $job, $mechanism) = @_;

    if (! ref($job)) {
        $job = $self->_get_doc($job);
    }
    return unless $job;

    if ($job->{isParallel}) {
        print "Job is parallel ".scalar(@{$job->{cmdline}})."\n" if ($main::main::DEBUG);
        for (my $i = 0; $i < @{$job->{cmdline}}; $i++) {
            $self->_schedule_single_job($job, $i, $mechanism);
        }
    } else {
        $self->_schedule_single_job($job, undef, $mechanism);
    }
}

sub _schedule_single_job {
    my($self, $job, $i, $mechanism) = @_;

    my $job_id = $job->{_id};
    my $queued_job_id;
    if ($mechanism eq 'lsf') {
        my $queue = $job->{'queueId'};
        my $cmdline = "$job_runner $job_id" . ( defined($i) ? " $i" : '');
        my $output = `bsub -B -N -q $queue $cmdline`;
        # Job <4791315> is submitted to queue <short>.
        ($queued_job_id) = ($output =~ m/Job \<(\d+)\> is/);

    } elsif ($mechanism eq 'fork') {
        $queued_job_id = fork();
        if (defined($queued_job_id) && !$queued_job_id) {
            exec($job_runner,$job_id, (defined($i) ? $i : () ) ) || die "exec failed: $!";
        }
    } elsif ($mechanism eq 'null') {
        $queued_job_id = 'fake';
    }

    $self->job_is_scheduled($job_id, $i, $queued_job_id);

    if ($mechanism eq 'null') {
        $self->job_is_running($job_id, $i, $$);
        $self->job_is_done($job_id, $i, result => 0);
    }
}


# job_id is a child job - tell it to decrement its waiting-on counter
sub signalChildJob {
    my($self, $job_id) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'parentIsDone', $job_id);
    my $req = HTTP::Request->new(PUT => $uri);
    while(1) {
        my $resp = eval { $self->{'server'}->request($req) };
        if ($@) {
            if ($@ =~ m/409/) {
                # update conflict, try again
                redo;
            } else {
                # somethiong else went wrong, rethrow the exception
                die $@;
            }
        } else {
            #no exception, we're done
            last;
        }
    }
}

sub add_dependant {
    my($self, $parent_job_id, $dep_job_id) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'addDependant', $parent_job_id);
    $uri .= "?jobId=$dep_job_id";
    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}

    

sub job_is_scheduled {
    my($self,$job_id, $i, $queued_job_id) = @_;
    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'scheduled', $job_id);
    $uri .= "?queueId=$queued_job_id";
    $uri .= "&i=$i" if (defined $i);

    if ($main::DEBUG) {
        print "Job $job_id ";
        print " with index $i" if (defined $i);
        print " scheduled with queue id $queued_job_id\n";
    }

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}


sub job_is_running {
    my($self,$job_id, $i, $pid) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'running', $job_id);
    $uri .= "?pid=$pid";
    $uri .= "&i=$i" if (defined $i);

    if ($main::DEBUG) {
        print "Job $job_id ";
        print " with index $i" if (defined $i);
        print " running with pid $pid\n";
    }
        
    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}


sub job_is_done {
    my($self, $job_id, $i, %params) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem' ) {
        next unless exists($params{$key});
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);
    $uri .= "&i=$i" if (defined $i);

    if ($main::DEBUG) {
        print "Job $job_id ";
        print " with index $i" if (defined $i);
        print " done with result ".$params{result}."\n";
    }

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}

sub job_is_crashed {
    my($self, $job_id, $i, %params) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem', 'signal', 'coredump' ) {
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);
    $uri .= "&i=$i" if (defined $i);

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}


sub changes_for_scheduler {
    my $self = shift;
    my $since = shift;

    my $design_doc_name = (split('/',$designDoc))[1];
    my $filter_name = $design_doc_name . '/readyToRun';
    my $uri = join('/', $self->{'uri'}, "_changes?feed=continuous&filter=$filter_name&include_docs=true");
    if ($since) {
        $uri .= "&since=$since";
    }
    my $request = HTTP::Request->new(GET => $uri);

    my $fh = IO::Socket::INET->new($self->{'host'});
    $fh->print($request->as_string);

    # read in the whole response
    while (1) {
        my $resp = $self->_read_line_from_fh($fh);
        $resp =~ s/\r|\n//g;  # Remove newline sequence
        last unless ($resp);
    }

    return $fh;
}

sub json_decode {
    my $self = shift;
    $self->{'json'}->decode(shift);
}

sub _read_line_from_fh {
    my($self, $fh) = @_;

    my $line = '';
    my $read;
    while($read = $fh->sysread($line, 1, length($line))) {
        if ($line =~ m/\n$/) {
            return $line;
        }
    }
    return $line;
}

sub read_all_lines_from_fh {
    my($self,$fh) = @_;

    my $data = '';
    my $datalen = 0;

    while(1) {
        my $read = $fh->sysread($data, 4096, length($data));
        last if ($data =~ m/\n$/);
        die "read 0 bytes from fh: $!" if (! $read);
        $datalen += $read;
    }

    if ($data !~ m/\n$/) {
        die "Read a partial line: >>>$data<<<\n";
    }

    return $data;
}


sub current_update_seq {
    my $self = shift;
    my $req = HTTP::Request->new(GET => $self->{'uri'});
    my $resp = $self->{'server'}->request($req);

    my $data = $self->json_decode($resp->content);
    return $data->{'update_seq'};
}
    
sub number_of_docs_in_db {
    my $self = shift;
    my $uri = join('/', $self->{'uri'}, '_all_docs?key="{}"');  #purposefully matches nothing
    my $req = HTTP::Request->new(GET => $uri);
    my $resp = $self->{server}->request($req);
    my $data = $self->json_decode($resp->content);
    return $data->{total_rows};
}

1;
