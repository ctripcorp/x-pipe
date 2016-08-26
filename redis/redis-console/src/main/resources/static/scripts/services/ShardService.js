services.service('ShardService', ['$resource', '$q', function ($resource, $q) {
    var resource = $resource('', {}, {
        find_cluster_dc_shards: {
            method: 'GET',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards',
            isArray: true
        },
        find_cluster_shards: {
            method: 'GET',
            url: '/console/clusters/:clusterName/shards',
            isArray: true
        },
        createShard: {
            method: 'POST',
            url: '/console/clusters/:clusterName/shards'
        },
        delete_shard: {
            method: 'DELETE',
            url: '/console/clusters/:clusterName/shards/:shardName'
        },
        bind_redis: {
            method: 'POST',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards/:shardName/redises'
        },
        unbind_redis: {
            method: 'DELETE',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards/:shardName/redises/:redisName'
        },
        update_redis: {
            method: 'PUT',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards/:shardName/redises/:redisName'
        }
    });

    function findClusterDcShards(clusterName, dcName) {
        var d = $q.defer();
        resource.find_cluster_dc_shards({
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

    function findClusterShards(clusterName) {
        var d = $q.defer();
        resource.find_cluster_shards({
        	clusterName: clusterName
        },
        	function (result) {
        		d.resolve(result);
        	}, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function create_shard(clusterName,shard) {
        var d = $q.defer();
        resource.createShard({
        	clusterName : clusterName
        }, shard,
        function (result) {
            d.resolve(result);
        }, function (result) {
            d.reject(result);
        });
        return d.promise;
    }

    function delete_shard(clusterName, shardName) {
        var d = $q.defer();
        resource.delete_shard({
                                  clusterName: clusterName,
                                  shardName: shardName
                              },
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;

    }

    function bind_redis(clusterName, dcName, shardName, redis) {
        var d = $q.defer();
        resource.bind_redis({
                                  clusterName: clusterName,
                                  dcName: dcName,
                                  shardName: shardName
                              },redis,
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function unbind_redis(clusterName, dcName, shardName, redisName) {
        var d = $q.defer();
        resource.unbind_redis({
                                  clusterName: clusterName,
                                  dcName: dcName,
                                  shardName: shardName,
                                  redisName: redisName
                              },
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function update_redis(clusterName, dcName, shardName, redisName, redis) {
        var d = $q.defer();
        resource.update_redis({
                                  clusterName: clusterName,
                                  dcName: dcName,
                                  shardName: shardName,
                                  redisName: redisName
                              }, redis,
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findClusterDcShards: findClusterDcShards,
        findClusterShards: findClusterShards,
        createShard: create_shard,
        deleteShard: delete_shard,
        bindRedis: bind_redis,
        unbindRedis: unbind_redis,
        updateRedis: update_redis
    }

}]);
