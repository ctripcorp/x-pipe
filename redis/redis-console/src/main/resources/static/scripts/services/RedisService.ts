angular
    .module('services')
    .service('RedisService', RedisService);

RedisService.$inject = ['$resource', '$q'];

function RedisService($resource, $q) {

    var resource = $resource('', {}, {
        update_shard_redis: {
            method: 'POST',
            url: '/console/clusters/:clusterName/dcs/:dcName/shards/:shardName',
        },
        migrate_keepers: {
            method: 'POST',
            url: '/console/keepercontainer/migration',
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

    function migrateKeepers(maxMigrationKeeperNum, targetKeeperContainer, srcKeeperContainer, migrationClusters) {
        var d = $q.defer();
        resource.migrate_keepers({},
                                {
                                    maxMigrationKeeperNum : maxMigrationKeeperNum,
                                    srcKeeperContainer : srcKeeperContainer,
                                    targetKeeperContainer : targetKeeperContainer,
                                    migrationClusters : migrationClusters
                                },
                                function (result) {
                                    d.resolve(result);
                                }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        updateShardRedis : updateShardRedis,
        migrateKeepers : migrateKeepers
    }
}