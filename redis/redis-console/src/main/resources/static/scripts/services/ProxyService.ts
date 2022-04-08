angular
    .module('services')
    .service('ProxyService', ProxyService);

ProxyService.$inject = ['$resource', '$q', '$http'];

function ProxyService($resource, $q, $http) {

    var resource = $resource('', {}, {
        get_proxy_chain:{
            method: 'GET',
            url: '/console/chain/:backupDcId/:clusterId/:shardId/:peerDcId',
        },
        get_proxy_chains: {
            method: 'GET',
            url: '/console/chain/:backupDcId/:clusterId',
        },
        exists_cluster_route: {
            method: 'GET',
            url: '/console/exist/route/dc/:currentDc/cluster/:clusterName'
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
        },
        close_proxy_chain: {
            method: 'DELETE',
            url: '/console/proxy/chain'
        },
        get_all_active_proxy_uris_by_dc: {
            method: 'GET',
            url: 'console/active/proxy/uri/:dcName',
            isArray: true
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

    function existsClusterRoute(currentDc, clusterName) {
        var d = $q.defer();
        resource.exists_cluster_route({
                currentDc: currentDc,
                clusterName: clusterName
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

    function closeProxyChain(chain) {
        var d = $q.defer();
        $http({
            method: 'DELETE',
            url: '/console/proxy/chain',
            data: [chain.activeDcTunnel.tunnelStatsResult.backend, chain.activeDcTunnel.tunnelStatsResult.backend],
            headers: {'Content-Type': 'application/json;charset=utf-8'}
        }).then(
            function successCallback(response) {
                d.resolve(response);
            },
            function errorCallback(reason) {
                d.reject(reason);
            });
        return d.promise;
    }

    function getAllActiveProxyUrisByDc(dcName) {
        var d = $q.defer();
        resource.get_all_active_proxy_uris_by_dc({
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
        loadAllProxyChainsForDcCluster : loadAllProxyChainsForDcCluster,
        existsClusterRoute: existsClusterRoute,
        getProxyChainHickwall: getProxyChainHickwall,
        getAllProxyInfo: getAllProxyInfo,
        getProxyTrafficHickwall: getProxyTrafficHickwall,
        closeProxyChain: closeProxyChain,
        getAllActiveProxyUrisByDc: getAllActiveProxyUrisByDc
    }
}
