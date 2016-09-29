services.service('RedisService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        update_shard_redis: {
            method: 'POST',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards/:shardName',
        }
    });

    function updateShardRedis(clusterName, dcName, shardName, shard) {
        var d = $q.defer();
        resource.update_shard_redis({
                                    clusterName: clusterName,
                                    dcName: dcName,
                                    shardName: shardName
                                }, shard,
                                function (result) {
                                    d.resolve(result);
                                }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        updateShardRedis: updateShardRedis
    }
}]);
