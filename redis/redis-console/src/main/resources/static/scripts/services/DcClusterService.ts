angular
    .module('services')
    .service('DcClusterService', DcClusterService);

DcClusterService.$inject = ['$resource', '$q'];

function DcClusterService($resource, $q) {
    var resource = $resource('', {}, {
        find_dc_cluster: {
            method: 'GET',
            url: '/console/dc-cluster/clusters/:clusterName/dcs/:dcName',
        },
        find_dc_clusters_by_cluster: {
            method: 'GET',
            url: '/console/dc-cluster/clusters/:clusterName',
            isArray: true
        }
    });

    function findDcCluster(clusterName, dcName) {
        var d = $q.defer();
        resource.find_dc_cluster({
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

        function findDcClusterByCluster(clusterName) {
            var d = $q.defer();
            resource.find_dc_clusters_by_cluster({
                    clusterName: clusterName,
                },
                function (result) {
                    d.resolve(result);
                }, function (result) {
                    d.reject(result);
                });
            return d.promise;
        }

    return {
        findDcCluster : findDcCluster,
        findDcClusterByCluster : findDcClusterByCluster,
    }
}
