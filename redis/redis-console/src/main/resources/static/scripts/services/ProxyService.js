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
        },
        exists_route_between: {
            method: 'GET',
            url: '/api/exist/route/active/:activeDcId/backup/:backupDcId'
        }
    });

    function loadAllProxyChainsForDcCluster(dcName, clusterName) {
        var d = $q.defer();
        resource.get_proxy_chains({
                backupDcId: dcName,
                clusterId: clusterName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function existsRouteBetween(activeDc, backupDc) {
        var d = $q.defer();
        resource.exists_route_between({
                activeDcId: activeDc,
                backupDcId: backupDc
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        loadAllProxyChainsForDcCluster : loadAllProxyChainsForDcCluster,
        existsRouteBetween: existsRouteBetween
    }
}]);