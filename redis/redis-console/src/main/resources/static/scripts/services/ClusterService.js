services.service('ClusterService', ['$resource', '$q', function ($resource, $q) {
    var resource = $resource('', {}, {
        load_cluster: {
            method: 'GET',
            url: '/clusters/:clusterName'
        }
    });
    return {
        load_cluster: function (clusterName) {
            var d = $q.defer();
            resource.load_cluster({
                                      clusterName: clusterName
                                  },
                                  function (result) {
                                      d.resolve(result);
                                  }, function (result) {
                    d.reject(result);
                });
            return d.promise;
        }
    }
}]);
