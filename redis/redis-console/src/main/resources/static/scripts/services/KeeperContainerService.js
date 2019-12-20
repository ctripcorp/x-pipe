services.service('KeeperContainerService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_availablekeepers_by_dc: {
            method: 'POST',
            url: '/console/dcs/:dcName/availablekeepers',
            isArray : true
        },
        find_active_kcs_by_dc_and_cluster: {
            method: 'GET',
            url: '/console/dcs/:dcName/cluster/:clusterName/activekeepercontainers',
            isArray : true
        },
        get_all_infos: {
            method: 'GET',
            url: '/console/keepercontainer/infos/all',
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

    function findAvailableKeepersByDc(dcName, shard) {
        var d = $q.defer();
        resource.find_availablekeepers_by_dc({
                            dcName: dcName
                        },shard,
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findAvailableKeepersByDcAndCluster(dcName, clusterName) {
            var d = $q.defer();
            resource.find_active_kcs_by_dc_and_cluster({
                                dcName: dcName,
                                clusterName: clusterName
                            },
                            function (result) {
                                d.resolve(result);
                            }, function (result) {
                    d.reject(result);
                });
            return d.promise;
    }

    function getAllInfos() {
        var d = $q.defer();
        resource.get_all_infos({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findAvailableKeepersByDc : findAvailableKeepersByDc,
        findAvailableKeepersByDcAndCluster : findAvailableKeepersByDcAndCluster,
        getAllInfos: getAllInfos,
    }
}]);
