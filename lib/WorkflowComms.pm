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

#$SIG{'CHLD'} = 'IGNORE';

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
    my $uri = $self->{'uri'};
    if ($doc->{_id}) {
        $method = 'PUT';
        $uri .= URI::Escape::uri_escape_utf8($doc->{_id});
    } else {
        $method = 'POST';
    }

    my $req = HTTP::Request->new($method => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);

    my $resp_headers = $self->json_decode($resp->content);
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

sub get_next_runnable_job {
    my $self = shift;

    my $uri = join('/', $self->{'uri'}, $designDoc, '_view', 'runnable');
    $uri .= '?limit=1';
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $self->{'server'}->request($req);

    my $data = $self->json_decode($resp->content);
    return eval { $data->{'rows'}->[0]->{'id'} };
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
    my($self, $job_id, $mechanism) = @_;

    my $job = $self->_get_doc($job_id);
    return unless $job;

    my $queued_job_id;
    if ($mechanism eq 'lsf') {
        my $queue = $job->{'queueId'};
        my $output = `bsub -B -N -q $queue $job_runner $job_id`;
        # Job <4791315> is submitted to queue <short>.
        ($queued_job_id) = ($output =~ m/Job \<(\d+)\> is/);

    } elsif ($mechanism eq 'fork') {
        $queued_job_id = fork();
        if (defined($queued_job_id) && !$queued_job_id) {
            exec($job_runner,$job_id) || die "exec failed: $!";
        }
    }

    $self->job_is_queued($job_id, $queued_job_id);
}

sub schedule_job_fake {
    my($self, $job_id, $mechanism) = @_;

    my $job = $self->_get_doc($job_id);
    return unless $job;

    my $queued_job_id = 'fake';
    $self->job_is_queued($job_id, $queued_job_id);
    $self->job_is_running($job_id, $$);
    $self->job_is_done($job_id, result => 0);
}


# job is a completed parent job doc or job id
# iterates over the dependants list and tells them to decrement
# their waiting-on counter
sub signalChildren {
    my($self, $job) = @_;

    # NOTE: We could also try making a view where the keys are job IDs
    # and the value is the dependant list.  Maybe that would be faster than
    # retrieving the whole doc?
    if (!ref($job)) {
        # it's a job id - get the doc
        $job = $self->_get_doc($job);
    }

    my $base_uri = join('/', $self->{'uri'}, $designDoc, '_update', 'parentIsDone/');
    foreach my $dep_job_id ( @{$job->{'dependants'}} ) {
        my $uri = $base_uri . $dep_job_id;
        my $req = HTTP::Request->new(PUT => $uri);
        my $resp = eval { $self->{'server'}->request($req) };
        if ($@) {
            if ($@ =~ m/409/) {
                # update conflict, try again
                redo;
            } else {
                # somethiong else went wrong, rethrow the exception
                die $@;
            }
        }
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

    

sub job_is_queued {
    my($self,$job_id, $queued_job_id) = @_;
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
        next unless exists($params{$key});
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

    my $data = $self->json_decode($resp->content);
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


sub Xchanges_for_scheduler {
    my $self = shift;
    my $since = shift;

    my $design_doc_name = (split('/',$designDoc))[1];
    my $filter_name = $design_doc_name . '/newReadyToRun';
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
    

1;
