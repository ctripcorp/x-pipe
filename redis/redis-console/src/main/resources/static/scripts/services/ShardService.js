services.service('ShardService', ['$resource', '$q', function ($resource, $q) {
    var resource = $resource('', {}, {
        find_shards: {
            method: 'GET',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards',
            isArray: true
        }
    });

    function findShards(clusterName, dcName) {
        var d = $q.defer();
        resource.find_shards({
                clusterName: clusterName,
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
        findShards: findShards
    }
    
}]);
