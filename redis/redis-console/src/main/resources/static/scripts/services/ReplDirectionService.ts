angular
    .module('services')
    .service('ReplDirectionService', ReplDirectionService);

ReplDirectionService.$inject = ['$resource', '$q'];

function ReplDirectionService($resource, $q) {
    var resource = $resource('', {}, {
        find_repl_direction_by_cluster_and_src_to_dc: {
            method: 'GET',
            url: '/console/repl-direction/cluster/:clusterName/src-dc/:srcDcName/to-dc/:toDcName',
        },
        find_repl_direction_by_cluster: {
            method: 'GET',
            url: '/console/repl-direction/cluster/:clusterName',
            isArray: true
        },
        get_all_repl_direction_infos: {
            method: 'GET',
            url: '/console/repl-direction/infos/all',
            isArray : true
        },
        complete_replication_by_repl_direction: {
            method: 'POST',
            url: '/console/repl-direction/repl-completion',
        },
    });

    function findReplDirectionByClusterAndSrcToDc(clusterName, srcDcName, toDcName) {
        var d = $q.defer();
        resource.find_repl_direction_by_cluster_and_src_to_dc({
                clusterName: clusterName,
                srcDcName: srcDcName,
                toDcName: toDcName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findReplDirectionByCluster(clusterName) {
        var d = $q.defer();
        resource.find_repl_direction_by_cluster({
                clusterName: clusterName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllReplDirectionInfos() {
        var d = $q.defer();
        resource.get_all_repl_direction_infos({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function completeReplicationByReplDirection(replDirection) {
        var d = $q.defer();
        resource.complete_replication_by_repl_direction({},
                replDirection,
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findReplDirectionByClusterAndSrcToDc : findReplDirectionByClusterAndSrcToDc,
        findReplDirectionByCluster : findReplDirectionByCluster,
        getAllReplDirectionInfos: getAllReplDirectionInfos,
        completeReplicationByReplDirection: completeReplicationByReplDirection,
    }
}
