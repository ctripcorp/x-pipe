angular
    .module('services')
    .service('ApplierService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_available_appliers_by_dc: {
            method: 'POST',
            url: '/console/dcs/:dcName/available-appliers',
            isArray : true
        },
        update_appliers: {
            method: 'POST',
            url: '/console/dcs/:dcName/clusters/:clusterName/shards/:shardName/repl-direction/:replDirectionId/appliers'
        }
    });

    function findAvailableAppliersByDc(dcName, sourceShard) {
        var d = $q.defer();
        resource.find_available_appliers_by_dc(
            {
                dcName: dcName
            },sourceShard,
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }
    function updateAppliers(dcName, clusterName, shardName, replDirectionId, sourceShard) {
        var d = $q.defer();
        resource.update_appliers({
                dcName: dcName,
                clusterName: clusterName,
                shardName: shardName,
                replDirectionId: replDirectionId
            },sourceShard,
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        findAvailableAppliersByDc : findAvailableAppliersByDc,
        updateAppliers : updateAppliers,
    }
}]);
