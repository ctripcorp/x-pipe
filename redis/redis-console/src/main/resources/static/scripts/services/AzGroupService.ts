angular
    .module('services')
    .service('AzGroupService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_all_az_groups: {
            method: 'GET',
            url: '/console/azGroups',
            isArray: true
        }
    });

    function getAllAzGroups() {
        var d = $q.defer();
        resource.get_all_az_groups({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        getAllAzGroups: getAllAzGroups
    }
}]);
