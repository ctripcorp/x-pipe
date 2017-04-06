services.service('KeeperContainerService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_activekeepercontainers_by_dc: {
            method: 'GET',
            url: '/console/dcs/:dcName/activekeepercontainers',
            isArray : true
        }
    });

    function findActiveKeeperContainersByDc(dcName) {
        var d = $q.defer();
        resource.find_activekeepercontainers_by_dc({
                            dcName: dcName
                        },
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }
    return {
        findActiveKeeperContainersByDc : findActiveKeeperContainersByDc
    }
}]);
