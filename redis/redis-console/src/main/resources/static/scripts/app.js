var services = angular.module('services', ['ngResource']);

var appUtil = angular.module('utils', ['toastr']);

var directive_module = angular.module('directive', ['toastr']);

var cluster_type = angular.module('cluster_type', [])

var index_module = angular.module('index', ['services', 'ui.router', 'toastr', 'utils','ngTable', 'directive', 'ngMaterial', 'localytics.directives', 'cluster_type']);

