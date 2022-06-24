angular
    .module('services')
    .service('AzService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_all_active_available_zone_infos_by_dc: {
            method: 'GET',
            url: '/console/az/dcs/:dcId',
            isArray : true
        }
    });

    function getAllActiveAvailableZoneInfosByDc(dcId) {
        var d = $q.defer();
        resource.get_all_active_available_zone_infos_by_dc({
                dcId : dcId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        getAllActiveAvailableZoneInfosByDc : getAllActiveAvailableZoneInfosByDc,
    }
}]);
