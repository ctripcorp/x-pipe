angular
    .module('services')
    .service('DcClusterService', DcClusterService);

DcClusterService.$inject = ['$resource', '$q'];

function DcClusterService($resource, $q) {
    var resource = $resource('', {}, {
        find_dc_cluster: {
            method: 'GET',
            url: '/console/clusters/:clusterName/dcs/:dcName',
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

    return {
        findDcCluster : findDcCluster,
    }
}
