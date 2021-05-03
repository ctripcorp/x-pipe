declare var angular: any;
declare var $: any;
declare var _: any;

declare var toastr: any;
declare var result: any;

declare var tmp_dc: any;
declare var dc: any;

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

require('./router');
require('./AppUtil');
require('./ClusterType');

let s = require.context('./services', false, /\.ts$/); s.keys().map(k => {s(k);});
let d = require.context('./directives', false, /\.ts$/); d.keys().map(k => {d(k);});
let c = require.context('./controllers', false, /\.ts$/); c.keys().map(k => {c(k);});
