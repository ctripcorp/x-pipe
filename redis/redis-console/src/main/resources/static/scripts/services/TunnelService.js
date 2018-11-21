services.service('TunnelService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_tunnels:{
            method: 'GET',
            url: '/console/proxy/:proxyIp/:proxyDcId',
            isArray: true
        }
    });

    function getAllTunnels(dcName, proxyIp) {
        var d = $q.defer();
        resource.get_tunnels({
                proxyDcId: dcName,
                proxyIp: proxyIp
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        getAllTunnels : getAllTunnels,
    }
}]);