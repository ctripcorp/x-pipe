angular
    .module('services')
    .service('ZoneService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_all_zones: {
            method: 'GET',
            url: '/api/zone/all',
            isArray: true
        }
    });

    function findAllZones() {
        var d = $q.defer();
        resource.find_all_zones({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findAllZones: findAllZones
    }
}]);
