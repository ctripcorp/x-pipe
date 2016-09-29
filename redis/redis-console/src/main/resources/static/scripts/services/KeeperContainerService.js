services.service('KeeperContainerService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_keeperContainers_by_dc: {
            method: 'GET',
            url: '/console/dcs/:dcName/keepercontainers',
            isArray : true
        }
    });

    function findKeeperContainersByDc(dcName) {
        var d = $q.defer();
        resource.find_keeperContainers_by_dc({
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
        findKeeperContainersByDc : findKeeperContainersByDc
    }
}]);
