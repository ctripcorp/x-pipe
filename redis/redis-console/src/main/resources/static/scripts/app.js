require('angular');
require('angular-resource');
require('angular-ui-router');
require('angular-toastr');
require('angular-animate');
require('angular-aria');
require('angular-messages');
require('angular-touch');
require('angular-material');

angular.module('services', ['ngResource']);
angular.module('utils', ['toastr']);
angular.module('directive', ['toastr']);
angular.module('cluster_type', [])
angular
    .module('index', ['services', 'ui.router', 'toastr', 'utils','ngTable', 'directive',
    'ngMaterial', 'localytics.directives', 'cluster_type']);

//same order as old index.html
//don't change order unless you know them well

let s = require.context('./services', false, /\.js$/); s.keys().map(k => {s(k);});
let d = require.context('./directives', false, /\.js$/); d.keys().map(k => {d(k);});
let c = require.context('./controllers', false, /\.js$/); c.keys().map(k => {c(k);});

require('./router');
require('./AppUtils');
require('./ClusterType');

