services.service('ProxyPingService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_collectors:{
            method: 'GET',
            url: '/console/proxy/collectors/:dcName',
            isArray: true
        },
        get_proxy_ping_hickwall_addr:{
            method: 'GET',
            url: '/console/proxy/ping/hickwall',
            isArray: false
        }
    });

    function getDcBasedCollectors(dcName) {
        var d = $q.defer();
        resource.get_collectors({
                dcName: dcName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getHickwallAddr() {
        var d = $q.defer();
        resource.get_proxy_ping_hickwall_addr({},
            function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        getDcBasedCollectors : getDcBasedCollectors,
        getHickwallAddr : getHickwallAddr
    }
}]);