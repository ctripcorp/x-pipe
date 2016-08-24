var services = angular.module('services', ['ngResource']);

var appUtil = angular.module('utils', ['toastr']);

var index_module = angular.module('index', ['services', 'ui.router', 'toastr', 'utils','oitozero.ngSweetAlert','ngTable']);

