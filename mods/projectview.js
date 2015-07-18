"use strict";

/**
 * Projectview API module
 *
 * Main tasks:
 * - TBD
 */


var rbUtil = require('../lib/rbUtil.js');
var URI = require('swagger-router').URI;
var uuid = require('cassandra-uuid').TimeUuid;

// TODO: move to module
var fs = require('fs');
var yaml = require('js-yaml');
var spec = yaml.safeLoad(fs.readFileSync(__dirname + '/projectview.yaml'));


// Projectview Service
function PJVS (options) {
    this.options = options;
    this.log = options.log || function(){};
}


PJVS.prototype.tableName = 'pvs_by_project';
PJVS.prototype.tableURI = function(domain) {
    return new URI([domain,'sys','table',this.tableName,'']);
};

// Get the schema for the revision table
PJVS.prototype.getTableSchema = function () {
    return {
        table: this.tableName,
        version: 1,
        attributes: {
            project: 'string',
            day: 'string',
            hour: 'string',
            agent_type: 'string',
            view_count: 'int',
        },
        index: [
            { attribute: 'project', type: 'hash' },
            { attribute: 'day', type: 'range', order: 'asc' },
            { attribute: 'hour', type: 'range', order: 'asc' },
            { attribute: 'agent_type', type: 'hash' }
        ],
    };
};


// /projectview
PJVS.prototype.listProjectview = function(restbase, req, options) {
    var rp = req.params;
    var revisionRequest;

    revisionRequest = restbase.get({
            uri: this.tableURI(rp.domain),
            body: {
                table: this.tableName,
                attributes: {
                    project: 'en.wikipedia',
                    agent_type: 'user'
                }
            }
        })
        .catch(function(e) {
            if (e.status !== 404) {
                throw e;
            }
        });
    return revisionRequest
    .then(function(res) {

        if (!res.headers) {
            res.headers = {};
        }

        return res;
    });
};


PJVS.prototype.insertProjectview = function(restbase, req) {
    var rp = req.params;

    return restbase.put({ // Save / update the revision entry
        uri: self.tableURI(rp.domain),
        body: {
            table: self.tableName,
            attributes: {
                project: 'en.wikipedia',
                agent_type: 'spider',
                day: '2015-05-01',
                hour: '00:00:00',
                view_count: '500'
            }
        }
    });
};


module.exports = function(options) {
    var pjvs = new PJVS(options);
    // XXX: add docs
    return {
        spec: spec,
        operations: {
            listProjectview: pjvs.listProjectview.bind(pjvs),
            insertProjectview: pjvs.insertProjectview.bind(pjvs),
        },
        resources: [
            {
                // projectview table
                uri: '/{domain}/sys/table/' + pjvs.tableName,
                body: pjvs.getTableSchema()
            }
        ]
    };
};
