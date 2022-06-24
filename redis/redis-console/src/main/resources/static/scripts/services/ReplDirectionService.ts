angular
    .module('services')
    .service('ReplDirectionService', ReplDirectionService);

ReplDirectionService.$inject = ['$resource', '$q'];

function ReplDirectionService($resource, $q) {
    var resource = $resource('', {}, {
        find_repl_direction_by_src_dc_and_to_dc: {
            method: 'GET',
            url: '/console/repl-direction/cluster/:clusterName/src-dc/:srcDcName/to-dc/:toDcName',
        }
    });

    function findReplDirectionBySrcDcAndToDc(clusterName, srcDcName, toDcName) {
        var d = $q.defer();
        resource.find_repl_direction_by_src_dc_and_to_dc({
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

    return {
        findReplDirectionBySrcDcAndToDc : findReplDirectionBySrcDcAndToDc,
    }
}
