angular
    .module('services')
    .service('ReplDirectionService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_all_repl_direction_infos: {
            method: 'GET',
            url: '/console/repl-direction/infos/all',
            isArray : true
        }
    });

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

    return {
        getAllReplDirectionInfos: getAllReplDirectionInfos,
    }
}]);
