package WorkflowComms;

use strict;
use warnings;

use LWP;
use URI;
use JSON;
use URI::Escape;
use IO::Socket;
use IO::Select;

use Data::Dumper;

my $designDoc = '_design/workflow-scheduler';
my $job_runner = '/gscuser/abrummet/newworkflow/task-based/job-runner.pl';

sub new {
    my $class = shift;
    
    my $self = bless {}, $class;
    $self->{'json'} = JSON->new->utf8;
    $self->{connections} = [];

    foreach my $uri ( @_ ) {
        my $server = LWP::UserAgent->new();
        my $uriobj = URI->new($uri);
        my $host = $uriobj->host_port;
        push @{$self->{connections}},
                {   server  => $server,
                    uri     => $uri,
                    host    => $host };
    }

    return $self;
}

# pick a connection at random
sub _connection  {
    my $self = shift;
    my $connection = $self->{connections}->[ int(rand( @{$self->{connections}} )) ];
    print "Sending to ".$connection->{host}."\n" if ($main::DEBUG);
    return $connection;
}

sub _get_doc {
    my($self, $id) = @_;

    my $conn = $self->_connection;
    my $uri = join('/', $conn->{'uri'}, $id);
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $conn->{'server'}->request($req);
    return eval { $self->json_decode($resp->content) };
}

sub _update_retry_conflict {
    my($self, $verb, $uri, $data) = @_;

    my $resp;
    while(! $resp) {
        my $conn = $self->_connection();
        my $send_uri = join('/', $conn->{'uri'}, $designDoc, $uri);
        my $req = HTTP::Request->new($verb => $send_uri);
        $req->header('Content-Type', 'application/json');
        $req->content($self->{'json'}->encode($data)) if ($data);

        my $server = $conn->{server};
        print "Sending $verb to $send_uri\n" if ($main::DEBUG);
        my $retry = 0;
        $resp = $server->request($req);
        print "    response code ".$resp->code()."\n" if ($main::DEBUG);
        $retry++;
        print STDERR "*** Tried $retry times to send $verb to $send_uri\n" if ($retry > 100);
        redo if ($resp and $resp->code() == 409);
    }
    return $resp;
}

sub enqueue {
    my($self, $job) = @_;

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'enqueue');
    my $uri = join('/', '_update', 'enqueue');
    my $resp = $self->_update_retry_conflict('POST', $uri, $job);
    #my $req = HTTP::Request->new(POST => $uri);
    #$req->header('Content-Type', 'application/json');
    #$req->content($self->{'json'}->encode($job));

    #my $resp = $conn->{'server'}->request($req);

    if ($resp->content =~ m/success: (\w+)/) {
        return $1;
    } else {
        return undef;
    }
}

sub get_runnable_jobs {
    my $self = shift;

    my $conn = $self->_connection();
    my $uri = join('/', $conn->{'uri'}, $designDoc, '_view', 'runnable');
    my $req = HTTP::Request->new(GET => $uri);
    $req->header('Content-Type', 'application/json');

    my $resp = $conn->{'server'}->request($req);

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

    my $job_id = $job->{_id};
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
    my($self, $job, $mechanism) = @_;

    if (! ref($job)) {
        $job = $self->_get_doc($job);
    }
    return unless $job;

    my $job_id = $job->{_id};
    my $queued_job_id = 'fake';
    print "Queueing job $job_id\n" if ($main::DEBUG);
    $self->job_is_queued($job_id, $queued_job_id);
    print "Running job $job_id\n" if ($main::DEBUG);
    $self->job_is_running($job_id, $$);
    print "Done job $job_id\n" if ($main::DEBUG);
    $self->job_is_done($job_id, result => 0);
}


# job_id is a child job - tell it to decrement its waiting-on counter
sub signalChildJob {
    my($self, $job_id) = @_;

    my $uri = join('/', '_update', 'parentIsDone', $job_id);
    my $resp = $self->_update_retry_conflict('PUT', $uri);

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'parentIsDone', $job_id);
    #my $req = HTTP::Request->new(PUT => $uri);
    #while(1) {
    #    my $resp = eval { $conn->{'server'}->request($req) };
    #    if ($@) {
    #        if ($@ =~ m/409/) {
    #            # update conflict, try again
    #            redo;
    #        } else {
    #            # somethiong else went wrong, rethrow the exception
    #            die $@;
    #        }
    #    } else {
    #        #no exception, we're done
    #        last;
    #    }
    #}
}

sub add_dependant {
    my($self, $parent_job_id, $dep_job_id) = @_;

    my $uri = join('/', '_update', 'addDependant', $parent_job_id);
    $uri .= "?jobId=$dep_job_id";
    my $resp = $self->_update_retry_conflict('PUT', $uri);

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'addDependant', $parent_job_id);
    #$uri .= "?jobId=$dep_job_id";
    #my $req = HTTP::Request->new(PUT => $uri);
    #my $resp = $conn->{'server'}->request($req);
    return $resp->content;
}

    

sub job_is_queued {
    my($self,$job_id, $queued_job_id) = @_;

print "job_id >>$job_id<< queued_job_id >>$queued_job_id<<\n";
    my $uri = join('/', '_update', 'scheduled', $job_id);
    $uri .= "?queueId=$queued_job_id";
    my $resp = $self->_update_retry_conflict('PUT', $uri);

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'scheduled', $job_id);
    #$uri .= "?queueId=$queued_job_id";
    #my $req = HTTP::Request->new(PUT => $uri);
    #my $resp = $conn->{'server'}->request($req);
    return $resp->content;
}


sub job_is_running {
    my($self,$job_id, $pid) = @_;

    my $uri = join('/', '_update', 'running', $job_id);
    $uri .= "?pid=$pid";
    my $resp = $self->_update_retry_conflict('PUT', $uri);

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'running', $job_id);
    #$uri .= "?pid=$pid";
#
#    my $req = HTTP::Request->new(PUT => $uri);
#    my $resp = $conn->{'server'}->request($req);
    return $resp->content;
}


sub job_is_done {
    my($self, $job_id, %params) = @_;

    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my $uri = join('/', '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem' ) {
        next unless exists($params{$key});
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);

    my $resp = $self->_update_retry_conflict('PUT', $uri);

    #my $req = HTTP::Request->new(PUT => $uri);
    #my $resp = $conn->{'server'}->request($req);
    return $resp->content;
}

sub job_is_crashed {
    my($self, $job_id, %params) = @_;

    my $uri = join('/', '_update', 'done', $job_id);
    #my $conn = $self->_connection();
    #my $uri = join('/', $conn->{'uri'}, $designDoc, '_update', 'done', $job_id);
    my @params;
    foreach my $key ( 'cpuTime', 'result', 'maxMem', 'signal', 'coredump' ) {
        my $val = $params{$key};
        push @params, "$key=$val";
    }
    $uri .= '?' . join('&', @params);

    my $resp = $self->_update_retry_conflict('PUT', $uri);
    #my $req = HTTP::Request->new(PUT => $uri);
    #my $resp = $conn->{'server'}->request($req);
    return $resp->content;
}


sub changes_for_scheduler {
    my $self = shift;
    my $since = shift;

    my $conn = $self->{connections}->[0];  # use the first one for changes
    my $design_doc_name = (split('/',$designDoc))[1];
    my $filter_name = $design_doc_name . '/readyToRun';
    my $uri = join('/', $conn->{'uri'}, "_changes?feed=continuous&filter=$filter_name&include_docs=true");
    if ($since) {
        $uri .= "&since=$since";
    }
    my $request = HTTP::Request->new(GET => $uri);

    my $fh = IO::Socket::INET->new($conn->{'host'});
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
    my $conn = $self->{connections}->[0];  # use the master connection
    my $req = HTTP::Request->new(GET => $conn->{'uri'});
    my $resp = $conn->{'server'}->request($req);

    my $data = $self->json_decode($resp->content);
    return $data->{'update_seq'};
}
    
sub number_of_docs_in_db {
    my $self = shift;
    my $conn = $self->{connections}->[0];  # use the master connection
    my $uri = join('/', $conn->{'uri'}, '_all_docs?key="{}"');  #purposefully matches nothing
    my $req = HTTP::Request->new(GET => $uri);
    my $resp = $conn->{server}->request($req);
    my $data = $self->json_decode($resp->content);
    return $data->{total_rows};
}

1;
