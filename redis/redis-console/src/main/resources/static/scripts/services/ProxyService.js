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
        },
        get_proxy_chain_hickwall_addr:{
            method: 'GET',
            url: '/console/proxy/chain/hickwall/:clusterId/:shardId',
            isArray: false
        },
        get_proxy_status_all: {
            methos: 'GET',
            url: '/console/proxy/status/all',
            isArray: true
        },
        get_proxy_traffic_hickwall: {
            method: 'GET',
            url: '/console/proxy/traffic/hickwall/:host/:port'
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

    function getProxyChainHickwall(clusterId, shardId) {
        var d = $q.defer();
        resource.get_proxy_chain_hickwall_addr({
                clusterId: clusterId,
                shardId: shardId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllProxyInfo() {
        var d = $q.defer();
        resource.get_proxy_status_all({
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getProxyTrafficHickwall(host, port) {
        var d = $q.defer();
        resource.get_proxy_traffic_hickwall({
                host: host,
                port: port
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
        existsRouteBetween: existsRouteBetween,
        getProxyChainHickwall: getProxyChainHickwall,
        getAllProxyInfo: getAllProxyInfo,
        getProxyTrafficHickwall: getProxyTrafficHickwall
    }
}]);