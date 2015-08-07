'use strict';

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, afterEach */

var assert = require('../../utils/assert.js');
var preq   = require('preq');
var server = require('../../utils/server.js');
// var P = require('bluebird');

describe('pageviews endpoints', function () {
    this.timeout(20000);

    before(function () { return server.start(); });

    var articleEndpoint = '/pageviews/per-article/en.wikipedia/spider/one/daily/2015070100/2015070300';
    var projectEndpoint = '/pageviews/per-project/en.wikipedia/spider/hourly/2015070100/2015070200';
    var topsEndpoint = '/pageviews/top/en.wikipedia/month';

    it('should return the expected per article data', function () {
        return preq.get({
            uri: server.config.baseURL + articleEndpoint
        })
        .then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });

    it('should return the expected per project data', function () {
        return preq.get({
            uri: server.config.baseURL + projectEndpoint
        })
        .then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });

    it('should return the expected tops data', function () {
        return preq.get({
            uri: server.config.baseURL + topsEndpoint
        })
        .then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });
});
