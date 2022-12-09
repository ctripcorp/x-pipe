angular
    .module('services')
    .service('SourceService', SourceService);

SourceService.$inject = ['$resource', '$q'];

function SourceService($resource, $q) {
    var resource = $resource('', {}, {
        find_cluster_dc_sources: {
            method: 'GET',
            url: '/console/clusters/:clusterName/dcs/:dcName/sources',
            isArray: true
        },
        find_cluster_dc_source: {
            method: 'GET',
            url: 'console/clusters/:clusterName/dcs/:dcName/shards/:shardName/source'
        }
    });

    function findClusterDcSources(clusterName, dcName) {
        var d = $q.defer();
        resource.find_cluster_dc_sources({
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

    function findClusterDcSource(clusterName, dcName, shardName) {
        var d = $q.defer();
        resource.find_cluster_dc_source({
                clusterName: clusterName,
                dcName: dcName,
                shardName: shardName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findClusterDcSources : findClusterDcSources,
        findClusterDcSource : findClusterDcSource,
    }
}
