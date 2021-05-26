import * as angular from "angular";
import uiRouter from "@uirouter/angularjs";
import { upgradeModule } from "@uirouter/angular-hybrid";

require('angular-resource');
require('angular-toastr');
require('angular-animate');
require('angular-aria');
require('angular-messages');
require('angular-touch');
require('angular-material');
require('ng-table')
require('angular-chosen');
require('angular-chosen-localytics');

angular.module('services', ['ngResource']);
angular.module('utils', ['toastr']);
angular.module('directive', ['toastr']);
angular.module('cluster_type', [])
angular
    .module('index', ['services', uiRouter, upgradeModule.name, 'toastr', 'utils','ngTable', 'directive',
        'ngMaterial', 'localytics.directives', 'cluster_type']);

require('./router');
require('./AppUtil');
require('./ClusterType');

let s = require.context('./services', false, /\.ts$/); s.keys().map(k => {s(k);});
let d = require.context('./directives', false, /\.ts$/); d.keys().map(k => {d(k);});
let c = require.context('./controllers', false, /\.ts$/); c.keys().map(k => {c(k);});
