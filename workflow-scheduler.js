var couchapp = require('couchapp');

var ddoc = {_id:'_design/workflow-scheduler',
            shows:{},
            updates:{},
            views:{},
            lists:{}
        };

exports.app = ddoc;


