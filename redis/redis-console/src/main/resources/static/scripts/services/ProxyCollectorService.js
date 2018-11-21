services.service('ProxyCollectorService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_collectors:{
            method: 'GET',
            url: '/console/proxy/collectors/:dcName',
            isArray: true
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


    return {
        getDcBasedCollectors : getDcBasedCollectors
    }
}]);