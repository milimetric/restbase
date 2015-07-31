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


PJVS.prototype.tableName = 'projectviews';
PJVS.prototype.tableURI = function(domain) {
    return new URI([domain,'sys','table',this.tableName,'']);
};

// Get the schema for the projectviews table
PJVS.prototype.getTableSchema = function () {
    return {
        table: this.tableName,
        version: 1,
        attributes: {
            project: 'string',
            day: 'string',
            hour: 'string',
            agent_type: 'string',
            view_count: 'int'
        },
        index: [
            { attribute: 'project', type: 'hash' },
            { attribute: 'day', type: 'range', order: 'asc' },
            { attribute: 'hour', type: 'range', order: 'asc' },
            { attribute: 'agent_type', type: 'hash' }
        ]
    };
};




PJVS.prototype.timeGranularProjectviews = function(restbase, req) {
    var rp = req.params;
    var projectviewRequest;

    var hourValue

    if (rp.timeGranularity == "hourly") {



    } else if (rp.timeGranularity == "daily") {


    } else {
        throw new rbUtil.HTTPError({
            status: 400,
            body: {
                type: 'invalidTimeGranularity',
                description: 'Invalid time granularity specified, should be hourly or daily.'
            }
        });
    }



    projectviewRequest = restbase.get({
        uri: this.tableURI(rp.domain),
        body: {
            table: this.tableName,
            attributes: {
                project: 'en.wikipedia',
                agent_type: 'spider',
            }
        }
    })
        .catch(function(e) {
            if (e.status !== 404) {
                throw e;
            }
        });
    return projectviewRequest
        .then(function(res) {

            if (!res.headers) {
                res.headers = {};
            }

            return res;
        });
};




/*// /projectview
PJVS.prototype.listProjectview = function(restbase, req) {
    var rp = req.params;
    var projectviewRequest;

    projectviewRequest = restbase.get({
            uri: this.tableURI(rp.domain),
            body: {
                table: this.tableName,
                attributes: {
                    project: 'en.wikipedia',
                    agent_type: 'spider',
                }
            }
        })
        .catch(function(e) {
            if (e.status !== 404) {
                throw e;
            }
        });
    return projectviewRequest
    .then(function(res) {

        if (!res.headers) {
            res.headers = {};
        }

        return res;
    });
};


PJVS.prototype.insertProjectview = function(restbase, req) {
    var rp = req.params;

    return restbase.put({ // Save / update the projectview entry
        uri: this.tableURI(rp.domain),
        body: {
            table: this.tableName,
            attributes: {
                project: 'en.wikipedia',
                agent_type: 'spider',
                day: '2015-05-01',
                hour: '00:00:00',
                view_count: '500'
            }
        }
    }).then(function() {
        return this.listProjectview(restbase, req);
    });
;
};*/


module.exports = function(options) {
    var pjvs = new PJVS(options);
    // XXX: add docs
    return {
        spec: spec,
        operations: {
            timeGranularProjectviews: pjvs.timeGranularProjectviews.bind(pjvs)
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
