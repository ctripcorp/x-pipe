services.service('ProxyService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_proxy_chain:{
            method: 'GET',
            url: '/console/chain/:backupDcId/:clusterId/:shardId',
            isArray: true
        },
        get_proxy_chains: {
            method: 'GET',
            url: '/console/chain/:backupDcId/:clusterId',
            isArray: true
        }
    });

    function loadAllProxyChainsForDcCluster(dcName, clusterName) {
        var d = $q.defer();
        resource.get_proxy_chains({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        loadAllProxyChainsForDcCluster : loadAllProxyChainsForDcCluster,
    }
}]);