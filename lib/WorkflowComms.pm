package WorkflowComms;

use LWP;
use URI;
use JSON;
use URI::Escape;
use IO::Socket;
use IO::Select;

my $designDoc = '_design/workflow-scheduler';
my $job_runner = '/gscuser/abrummet/newworkflow/task-based/job-runner.pl';

$SIG{'CHLD'} = 'IGNORE';

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

sub _save_doc {
    my($self, $doc) = @_;

    my $method;
    if ($doc->{_id}) {
        $method = 'PUT';
        $uri .= URI::Escape::uri_escape_utf8($doc->{_id});
    } else {
        $method = 'POST';
    }

    my $req = HTTP::Request->new($method => $self->{'uri'});
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);

    my $resp_headers = $self->{'json'}->decode($res->content);
    $doc->{_id} = $resp_headers->{id};
    $doc->{_rev} = $resp_headers->{rev};

    return $doc;
}

sub _get_doc {
    my($self, $id) = @_;

    my $uri = join('/', $self->{'uri'}, $id);
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);
    return eval { $self->{'json'}->decode($resp->content) };
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

sub get_next_runnable_job {
    my $self = shift;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_view', 'runnable');
    $uri .= '?limit=1';
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);

    my $data = $self->{'json'}->decode($resp->content);
    return eval { $data->{'rows'}->[0]->{'id'} };
}


sub schedule_job {
    my($self, $job_id, $mechanism) = @_;

    my $job = $self->_get_doc($job_id);
    return unless $job;

    my $queued_job_id;
    if ($mechanism eq 'lsf') {
        my $queue = $job->{'queueId'};
        my $output = `bsub -q $queue $job_runner $job_id`;
        # Job <4791315> is submitted to queue <short>.
        ($queued_job_id) = ($output =~ m/Job \<(\d+)\> is/);

    } elsif ($mechanism eq 'fork') {
        print "About to schedule via fork()\n";
        $queued_job_id = fork();
        print "queued job id $queued_job_id\n";
        if (defined($queued_job_id) && !$queued_job_id) {
            print "About to start job runner: $job_runner $job_id\n";
            exec($job_runner,$job_id);
            print "exec failed! $!\n";
            exit(1);
        }
    }

    print "Job scheduled queued id is $queued_job_id\n";
    
    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'scheduled', $job_id);
    $uri .= "?queueId=$queued_job_id";
    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}

sub job_is_running {
    my($self,$job_id, $pid) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'running', $job_id);
    $uri .= "?pid=$pid";

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}


sub job_is_done {
    my($self, $job_id, %params) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem' ) {
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}

sub job_is_crashed {
    my($self, $job_id, %params) = @_;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem', 'signal', 'coredump' ) {
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);

    my $req = HTTP::Request->new(PUT => $uri);
    my $resp = $self->{'server'}->request($req);
    return $resp->content;
}


sub remove_job_as_dependancy {
    my($self, $job_id) = @_;

    # first find which jobs depended on this one
    my $uri = join('/', $self->{'uri'}, $designDoc, '_view','dependancies');
    $uri .= '?key="' . $job_id . '"';
    my $req = HTTP::Request->new(GET => $uri);
    my $resp = $self->{'server'}->request($req);

    my $data = $self->{'json'}->decode($resp->content);
    my @dep_job_ids;
    foreach ( @{$data->{'rows'}} ) {
        push @dep_job_ids, $_->{'id'};
    }

    # now tell each of them that this nob is done
    my $base_update_uri = join('/', $self->{'uri'}, $designDoc, '_update', 'removeDependancy');
    foreach my $id (@dep_job_ids) {
        my $update_uri = $base_update_uri . "/$id?job=$job_id";
        my $req = HTTP::Request->new(PUT => $update_uri);
        my $resp = $self->{'server'}->request($req);
    }
}


sub changes_for_scheduler {
    my $self = shift;
    my $since = shift;

    my $design_doc_name = (split('/',$designDoc))[1];
    my $filter_name = $design_doc_name . '/finishedJobs';
    my $uri = join('/', $self->{'uri'}, "_changes?feed=continuous&filter=$filter_name");
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

sub get_finished_job_id_from_changes {
    my $self = shift;
    my $fh = shift;

    my $line = $self->_read_line_from_fh($fh);
    chomp($line);
    my $data = $self->{'json'}->decode($line);
    return $data->{'id'};
}


sub current_update_seq {
    my $self = shift;
    my $req = HTTP::Request->new(GET => $self->{'uri'});
    my $resp = $self->{'server'}->request($req);

    my $data = $self->{'json'}->decode($resp->content);
    return $data->{'update_seq'};
}
    

1;
