'use strict';

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, afterEach */

var assert = require('../../utils/assert.js');
var preq   = require('preq');
var server = require('../../utils/server.js');


describe('pageviews endpoints', function () {
    this.timeout(20000);

    //Start server before running tests
    before(function () { return server.start(); });

    var articleEndpoint = '/pageviews/per-article/en.wikipedia/spider/one/daily/2015070100/2015070300';
    var projectEndpoint = '/pageviews/per-project/en.wikipedia/spider/hourly/2015070100/2015070102';
    var topsEndpoint = '/pageviews/top/en.wikipedia/month';

    // Fake data insertion endpoints
    var insertArticleEndpoint = '/pageviews/insert-per-article/en.wikipedia/spider/one/daily/2015070200/100';
    var insertProjectEndpoint = '/pageviews/insert-per-project/en.wikipedia/spider/hourly/2015070101/10';
    var insertTopsEndpoint = '/pageviews/insert-top/en.wikipedia/month/1/one/2000';

    // Test Article Endpoint

    it('should return empty when no per article data is available', function () {
        return preq.get({
            uri: server.config.baseURL + articleEndpoint
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });

    it('should return the expected per article data after insertion', function () {
        return preq.get({
            uri: server.config.baseURL + insertArticleEndpoint
        }).then(function (res){
            return preq.get({
                uri: server.config.baseURL + articleEndpoint
            })
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 1);
        });
    });


    // Test Project Endpoint

    it('should return empty when no per project data is available', function () {
        return preq.get({
            uri: server.config.baseURL + projectEndpoint
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });

    it('should return the expected per project data after insertion', function () {
        return preq.get({
            uri: server.config.baseURL + insertProjectEndpoint
        }).then(function (res){
            return preq.get({
                uri: server.config.baseURL + projectEndpoint
            })
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 1);
        });
    });


    // Test Top Endpoint

    it('should return empty when no tops data is available', function () {
        return preq.get({
            uri: server.config.baseURL + topsEndpoint
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 0);
        });
    });

    it('should return the expected tops data after insertion', function () {
        return preq.get({
            uri: server.config.baseURL + insertTopsEndpoint
        }).then(function (res){
            return preq.get({
                uri: server.config.baseURL + topsEndpoint
            })
        }).then(function (res) {
            assert.deepEqual(res.body.items.length, 1);
        });
    });
});
