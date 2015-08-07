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


var tables = {
        article: 'pageviews.per.article',
        project: 'pageviews.per.project',
        tops: 'top.pageviews',
    },
    tableURI = function(domain, tableName) {
        return new URI([domain, 'sys', 'table', tableName, '']);
    },
    tableSchemas = {
        article: {
            table: tables.article,
            version: 1,
            attributes: {
                project     : 'string',
                article     : 'string',
                agent       : 'string',
                granularity : 'string',
                // the hourly timestamp will be stored as YYYYMMDDHH
                timestamp   : 'string',
                views       : 'int'
            },
            index: [
                { attribute: 'project', type: 'hash' },
                { attribute: 'article', type: 'hash' },
                { attribute: 'agent', type: 'hash' },
                { attribute: 'granularity', type: 'hash' },
                { attribute: 'timestamp', type: 'range', order: 'asc' },
            ]
        },
        project: {
            table: tables.project,
            version: 1,
            attributes: {
                project     : 'string',
                agent       : 'string',
                granularity : 'string',
                // the hourly timestamp will be stored as YYYYMMDDHH
                timestamp   : 'string',
                views       : 'int'
            },
            index: [
                { attribute: 'project', type: 'hash' },
                { attribute: 'agent', type: 'hash' },
                { attribute: 'granularity', type: 'hash' },
                { attribute: 'timestamp', type: 'range', order: 'asc' },
            ]
        },
        tops: {
            table: tables.tops,
            version: 1,
            attributes: {
                project     : 'string',
                timespan    : 'string',
                // format for this is a json array: [{rank: 1, article: <<title>>, views: 123}, ...]
                articles    : 'string'
            },
            index: [
                { attribute: 'project', type: 'hash' },
                { attribute: 'timespan', type: 'hash' },
            ]
        }
    };

/* general handler functions */
var queryCatcher = function (e) {
        if (e.status !== 404) {
            throw e;
        }
    },
    queryResponser = function (res) {
        if (!res) {
            // this could mean there was a 404 error above, which could mean the query found no data
            return {};
        }

        res.headers = res.headers || {};
        return res;
    };


PJVS.prototype.pageviewsForArticle = function (restbase, req) {
    var rp = req.params,
        dataRequest;

    dataRequest = restbase.get({
        uri: tableURI(rp.domain, tables.article),
        body: {
            table: tables.article,
            attributes: {
                project: rp.project,
                agent: rp.agent,
                article: rp.article,
                granularity: rp.granularity,
                timestamp: { between: [rp.start, rp.end] },
            }
        }

    }).catch(queryCatcher);

    return dataRequest.then(queryResponser);
};

PJVS.prototype.pageviewsForProjects = function (restbase, req) {
    var rp = req.params,
        dataRequest;

    dataRequest = restbase.get({
        uri: tableURI(rp.domain, tables.project),
        body: {
            table: tables.project,
            attributes: {
                project: rp.project,
                agent: rp.agent,
                granularity: rp.granularity,
                timestamp: { between: [rp.start, rp.end] },
            }
        }

    }).catch(queryCatcher);

    return dataRequest.then(queryResponser);
};

PJVS.prototype.pageviewsForTops = function (restbase, req) {
    var rp = req.params,
        dataRequest;

    console.log(rp);
    dataRequest = restbase.get({
        uri: tableURI(rp.domain, tables.tops),
        body: {
            table: tables.tops,
            attributes: {
                project: rp.project,
                timespan: rp.timespan,
            }
        }

    }).catch(queryCatcher);

    return dataRequest.then(queryResponser);
};


/* NOTE: Is this needed for more than just test data? */
var moment = require('moment');
PJVS.prototype.insertPageviewsForArticleTestData = function(restbase, req) {
    var rp = req.params,
        start = moment('2014-07-01'),
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
                uri: tableURI(rp.domain, tables.article),
                body: {
                    table: tables.article,
                    attributes: attributes,
                }
            });

            if (dateIterator.get('hours') === 0) {
                attributes.granularity = 'daily';
                attributes.views = 12000;

                lastPromise = restbase.put({ // Save / update the pageviews entry
                    uri: tableURI(rp.domain, tables.article),
                    body: {
                        table: tables.article,
                        attributes: attributes,
                    }
                });
            }
        }
    });

    return lastPromise;
};


PJVS.prototype.insertPageviewsForProjectTestData = function(restbase, req) {
    var rp = req.params,
        start = moment('2014-07-01'),
        end = moment('2015-08-01'),
        lastPromise,
        dateIterator;

    ['en.wikipedia', 'es.wikipedia', 'ro.wikipedia', 'fr.wikipedia', 'all'].forEach(function (project) {
        for (dateIterator = start; dateIterator.isBefore(end); dateIterator.add('hours', 1)) {

            var attributes = {
                    project: project,
                    granularity: 'hourly',
                    agent: (Math.floor(Math.random() * 10)) % 3 === 0 ? 'spider' : 'user',
                    timestamp: dateIterator.format('YYYYMMDDHH'),
                    views: project === 'all' ? 50000 : 200000
                };

            lastPromise = restbase.put({ // Save / update the pageviews entry
                uri: tableURI(rp.domain, tables.project),
                body: {
                    table: tables.project,
                    attributes: attributes,
                }
            });

            if (dateIterator.get('hours') === 0) {
                attributes.granularity = 'daily';
                attributes.views = project === 'all' ? 1200000 : 4800000;

                lastPromise = restbase.put({ // Save / update the pageviews entry
                    uri: tableURI(rp.domain, tables.project),
                    body: {
                        table: tables.project,
                        attributes: attributes,
                    }
                });
            }
        }
    });

    return lastPromise;
};


PJVS.prototype.insertPageviewsForTopsTestData = function(restbase, req) {
    var rp = req.params,
        lastPromise;

    ['en.wikipedia', 'es.wikipedia', 'ro.wikipedia', 'fr.wikipedia', 'all'].forEach(function (project) {
        ['year', 'month', 'day'].forEach(function (timespan) {

            lastPromise = restbase.put({ // Save / update the pageviews entry
                uri: tableURI(rp.domain, tables.tops),
                body: {
                    table: tables.tops,
                    attributes: {
                        project: project,
                        timespan: timespan,
                        articles: JSON.stringify([
                            {rank: 1, article: 'one ' + timespan, views: 125},
                            {rank: 2, article: 'two ' + timespan, views: 114},
                            {rank: 3, article: 'thr ' + timespan, views: 103},
                        ]),
                    },
                }
            });
        });
    });

    return lastPromise;
};


module.exports = function(options) {
    var pjvs = new PJVS(options);

    return {
        spec: spec,
        operations: {
            pageviewsForArticle: pjvs.pageviewsForArticle.bind(pjvs),
            pageviewsForProjects: pjvs.pageviewsForProjects.bind(pjvs),
            pageviewsForTops: pjvs.pageviewsForTops.bind(pjvs),

            insertPageviewsForArticleTestData: pjvs.insertPageviewsForArticleTestData.bind(pjvs),
            insertPageviewsForProjectTestData: pjvs.insertPageviewsForProjectTestData.bind(pjvs),
            insertPageviewsForTopsTestData: pjvs.insertPageviewsForTopsTestData.bind(pjvs),
        },
        resources: [
            {
                // pageviews per article table
                uri: '/{domain}/sys/table/' + tables.article,
                body: tableSchemas.article,
            }, {
                // pageviews per project table
                uri: '/{domain}/sys/table/' + tables.project,
                body: tableSchemas.project,
            }, {
                // top pageviews table
                uri: '/{domain}/sys/table/' + tables.tops,
                body: tableSchemas.tops,
            }
        ]
    };
};
