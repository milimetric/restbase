'use strict';

/**
 * Pageviews API module
 *
 * Main tasks:
 * - TBD
 */


var URI = require('swagger-router').URI;

// TODO: move to module
var fs = require('fs');
var yaml = require('js-yaml');
var path = require('path');
var spec = yaml.safeLoad(fs.readFileSync(path.join(__dirname, '/pageviews.yaml')));


// Pageviews Service
function PJVS (options) {
    this.options = options;
    this.log = options.log || function(){};
}


PJVS.prototype.tableName = 'pageviews';
PJVS.prototype.tableURI = function(domain) {
    return new URI([domain, 'sys', 'table', this.tableName, '']);
};


// Get the schema for the pageviews table
PJVS.prototype.getTableSchema = function () {
    return {
        table: this.tableName,
        version: 1,
        attributes: {
            project     : 'string',
            article     : 'string',
            agent       : 'string',
            granularity : 'string',
            // the hourly timestamp will be stored as YYYYMMDDHH
            timestamp   : 'string',
            views: 'int'
        },
        index: [
            { attribute: 'project', type: 'hash' },
            { attribute: 'article', type: 'hash' },
            { attribute: 'agent', type: 'hash' },
            { attribute: 'granularity', type: 'hash' },
            { attribute: 'timestamp', type: 'range', order: 'asc' },
        ]
    };
};


PJVS.prototype.pageviewsForArticle = function (restbase, req) {
    var rp = req.params,
        dataRequest;

    dataRequest = restbase.get({
        uri: this.tableURI(rp.domain),
        body: {
            table: this.tableName,
            attributes: {
                project: rp.project,
                agent: rp.agent,
                article: rp.article,
                granularity: rp.granularity,
                timestamp: { between: [rp.start, rp.end] },
            }
        }

    }).catch(function (e) {
        if (e.status !== 404) {
            throw e;
        }
    });

    return dataRequest.then(function (res) {
        if (!res) {
            // this could mean there was a 404 error above, which could mean the query found no data
            return {};
        }

        res.headers = res.headers || {};
        return res;
    });
};


/* Is this needed for more than just test data? */
var moment = require('moment');
PJVS.prototype.insertPageviewsForArticleTestData = function(restbase, req) {
    var rp = req.params,
        self = this,
        start = moment('2015-07-01'),
        end = moment('2015-08-01'),
        lastPromise,
        dateIterator;

    ['one', 'two', 'three', 'four', 'five'].forEach(function (article) {
        for (dateIterator = start; dateIterator.isBefore(end); dateIterator.add('hours', 1)) {

            var attributes = {
                    project: 'en.wikipedia',
                    article: article,
                    granularity: 'hourly',
                    agent: (Math.floor(Math.random() * 10)) % 3 === 0 ? 'spider' : 'user',
                    timestamp: dateIterator.format('YYYYMMDDHH'),
                    views: 500
                };

            lastPromise = restbase.put({ // Save / update the pageviews entry
                uri: self.tableURI(rp.domain),
                body: {
                    table: self.tableName,
                    attributes: attributes,
                }
            });

            if (dateIterator.get('hours') === 0) {
                attributes.granularity = 'daily';
                attributes.views = 12000;

                lastPromise = restbase.put({ // Save / update the pageviews entry
                    uri: self.tableURI(rp.domain),
                    body: {
                        table: self.tableName,
                        attributes: attributes,
                    }
                });
            }
        }
    });

    return lastPromise;
};


module.exports = function(options) {
    var pjvs = new PJVS(options);
    // XXX: add docs
    return {
        spec: spec,
        operations: {
            pageviewsForArticle: pjvs.pageviewsForArticle.bind(pjvs),
            insertPageviewsForArticleTestData: pjvs.insertPageviewsForArticleTestData.bind(pjvs),
        },
        resources: [
            {
                // pageviews table
                uri: '/{domain}/sys/table/' + pjvs.tableName,
                body: pjvs.getTableSchema()
            }
        ]
    };
};
