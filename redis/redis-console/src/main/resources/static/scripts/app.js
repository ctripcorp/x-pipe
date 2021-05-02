angular.module('services', ['ngResource']);

angular.module('utils', ['toastr']);

angular.module('directive', ['toastr']);

angular.module('cluster_type', [])

angular.module('index', ['services', 'ui.router', 'toastr', 'utils','ngTable', 'directive',
    'ngMaterial', 'localytics.directives', 'cluster_type']);

