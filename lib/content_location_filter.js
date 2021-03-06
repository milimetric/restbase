'use strict';

const HyperSwitch = require('hyperswitch');
const HTTPError = HyperSwitch.HTTPError;
const URI = HyperSwitch.URI;
const mwUtil = require('./mwUtil');

function isAbsoluteRedirect(location) {
    return /^https?:/.test(location);
}

function resolveRelativeRedirect(req, res) {
    const pathArr = req.uri.path.slice(0, -1).map(encodeURIComponent);
    pathArr.unshift('');
    pathArr.push(res.headers.location);
    return pathArr.join('/');
}

function resolvedRedirectResponse(hyper, req, res, redirectNum = 0) {
    let contentURI;
    if (isAbsoluteRedirect(res.headers.location)) {
        contentURI = res.headers.location;
    } else {
        contentURI = resolveRelativeRedirect(req, res);
    }
    return hyper.request({
        method: req.method,
        uri: new URI(contentURI)
        // We don't pass the `origin` header further, so possible
        // redirect looping will be handled by normal redirect limiting in `preq`
    })
    .then((res) => {
        if (res.status >= 300) {
            if (redirectNum > 10) {
                throw new HTTPError({
                    status: 504,
                    body: {
                        type: 'gateway_timeout',
                        detail: 'Exceeded max redirects'
                    }
                });
            }
            redirectNum++;
            return resolvedRedirectResponse(hyper, req, res, redirectNum);
        }
        return res;
    })
    .tap((res) => {
        res.headers = res.headers || {};
        res.headers['cache-control'] = 'no-cache';
        res.headers.vary = res.headers.vary ? `${res.headers.vary}, origin` : 'origin';
    });
}

module.exports = (hyper, req, next) => {
    if (req.method !== 'get') {
        return next(hyper, req);
    } else {
        const attachLocation = (res) => {
            res.headers = res.headers || {};
            if (res.status === 301 || res.status === 302) {
                if (mwUtil.isCrossOrigin(req)) {
                    return resolvedRedirectResponse(hyper, req, res);
                }
                return res;
            }
            return mwUtil.getSiteInfo(hyper, req)
            .then((siteInfo) => {
                Object.assign(res.headers, {
                    'content-location': siteInfo.baseUri +
                    new URI(req.uri.path.slice(2)) +
                    mwUtil.getQueryString(req)
                });
                return res;
            });
        };

        return next(hyper, req)
        .then(attachLocation, attachLocation)
        .tap((res) => {
            if (res.status >= 400) {
                throw res;
            }
        });
    }
};
