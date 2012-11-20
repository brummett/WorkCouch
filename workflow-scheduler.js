var couchapp = require('couchapp');

var ddoc = {_id:'_design/workflow-scheduler',
            shows:{},
            updates:{},
            views:{},
            lists:{},
            filters:{},
        };

module.exports = ddoc;

ddoc.validate_doc_update = function(doc) {
    return true;
}

// Submit a job
// Accepts params:
//      workflowId  -   all jobs in one workflow share the same workflowId
//      clusterId   -   identifies a scheduler daemon
//      queueId     -   LSF queue
//      label       -   human readable name for this job
//      cmdline     -   what to exec
//      id          -   give the job and ID - can be undefined to use random UUID
//      status      -   initial job status, default is 'waiting'
ddoc.updates.enqueue = function(doc,req) {

    // if req.form is empty, then assumme req.body contains a JSON encoded
    // object, and we'll decode it and put the results into req.form
    //if (Object.keys(req.form).length === 0) {
    //    req.form = JSON.parse(req.body);
    //}
    var formIsEmpty = true;
    for (var prop in req.form) {
        formIsEmpty = false;
        break;
    }
    if (formIsEmpty) {
        req.form = JSON.parse(req.body);
    }

    if (doc) {
        return [null, 'error: Cannot enqueue an already existing job'];
    }

    doc = {};
    doc.workflowId  = req.query.workflowId || req.form.workflowId;
    doc.clusterId   = req.query.clusterId || req.form.clusterId;
    doc.queueId     = req.query.queueId || req.form.queueId;
    doc.label       = req.query.label || req.form.label;
    doc.cmdline     = req.query.cmdline || req.form.cmdline;
    doc.waitingOn   = parseInt(req.query.waitingOn) || parseInt(req.form.waitingOn) || 0;
    doc.submitTime  = Date.now();  // milliseconds

    //if ('depends' in req.query) {
    //    doc.depends     = [ req.query.depends ];
    //} else if ('depends' in req.form) {
    //    doc.depends     = req.form.depends;
    //}

    if ('id' in req.query) {
        doc._id = req.query.id;
    } else if ('id' in req.form) {
        doc._id = req.form.id;
    } else {
        doc._id = req.uuid;
    }

    if ('status' in req.query) {
        doc.status = req.query.status;
    } else if ('status' in req.form) {
        doc.status = req.form.status;
    } else {
        doc.status = 'waiting';
    }

    return [doc, 'success: ' + doc._id];
};

// Mark that a job is scheduled
// The sceduler uses this when it's been submitted to the underlying job scheduler
// Accepts params:
//      queueId -   The LSF job ID
ddoc.updates.scheduled = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    } else if (doc.status != 'waiting') {
        return [null, 'error: Job status is not "waiting"'];
    }

    doc.status = 'scheduled';
    doc.scheduleTime = Date.now();
    doc.queueId = req.query.queueId || req.form.queueId;
    return [doc, 'success'];
}

// Mark that a job is being worked on
// Accepts params:
//      hostname    -   hostname it's running on, default is req.peer
//      pid         -   process ID of the running job
ddoc.updates.running = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    } else if (doc.status != 'scheduled') {
        return [null, 'error: Job status is not "scheduled"'];
    }

    doc.status = 'running';
    doc.startTime = Date.now();
    doc.hostname = req.query.hostname || req.form.hostname || req.peer;
    doc.pid = req.query.pid || req.form.pid;
    return [doc, 'success'];
};

// Mark that a job finished running
// Accepts params:
//      result      -   exit code of the program
//      cpuTime     -   How much CPU time the job used
//      maxMem      -   Max memory used by the job
ddoc.updates.done = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    } else if (doc.status != 'running') {
        return [null, 'error: Job status is not "running"'];
    }

    doc.status = 'done';
    doc.doneTime = Date.now();
    doc.result = req.query.result || req.form.result;
    doc.cpuTime = parseFloat(req.query.cpuTime || req.form.cpuTime || 0);
    doc.maxMem = parseInt(req.query.maxMem || req.form.maxMem || 0);
    return [doc, 'success'];
};

// Mark the job as crashed
// Accpets params:
//      result      -   exit code of the program
//      signal      -   signal the job exited from
//      coredump    -   true if a coredump was generated
//      cpuTime     -   How much CPU time the job used
//      maxMem      -   Max memory used by the job
ddoc.updates.crashed = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    } else if (doc.status != 'running') {
        return [null, 'error: Job status is not "running"'];
    }

    doc.status = 'crashed';
    doc.doneTime = Date.now();
    doc.result = req.query.result || req.form.result;
    doc.signal = req.query.signal || req.form.signal;
    doc.cpuTime = parseFloat(req.query.cpuTime || req.form.cpuTime || 0);
    doc.maxMem = parseInt(req.query.maxMem || req.form.maxMem || 0);
    if ('coredump' in req.query) {
        doc.coredump = req.query.coredump ? true : false;
    } else if ('coredump' in req.form) {
        doc.coredump = req.form.coredump ? true : false;
    }
    return [doc,'success'];
};


// Decrement the waitingOn counter for a dependant job
ddoc.updates.parentIsDone = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    }

    doc.waitingOn--;
    return [doc, 'success'];
}

// Add the given job ID as a dependant of this parent job
ddoc.updates.addDependant = function(doc,req) {
    if (!doc) {
        return [null, 'error: No job matching that id'];
    }

    doc.dependants = doc.dependants || [];
    doc.dependants.push(req.query.jobId);
    return [doc, 'success'];
}

// Return all the jobs where waitingOn is 0
ddoc.views.runnable = {
    'map': function(doc) {
        if ((doc.status === 'waiting') && (! doc.waitingOn)) {
            emit(doc._id, null);
        }
    }
};

ddoc.filters.readyToRun = function(doc, req) {
    if (doc.status === 'done') {
        // a job just finished
        return true;
    } else if ( (doc.status === 'waiting') && (! doc.waitingOn)) {
        // A job is now waiting on no new jobs
        return true;
    } else {
        return false;
    }
}
