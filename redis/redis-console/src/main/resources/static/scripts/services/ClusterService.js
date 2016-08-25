services.service('ClusterService', ['$resource', '$q', function ($resource, $q) {
    var resource = $resource('', {}, {
        load_cluster: {
            method: 'GET',
            url: '/console/clusters/:clusterName'
        },
        find_cluster_dcs: {
            method: 'GET',
            url: '/console/clusters/:clusterName/dcs',
            isArray: true
        },
        find_all_clusters: {
            method: 'GET',
            url: '/console/clusters/all',
            isArray: true
        },
        create_cluster: {
            method: 'POST',
            url: '/console/clusters'
        },
        update_cluster: {
            method: 'PUT',
            url: '/console/clusters/:clusterName'
        },
        delete_cluster: {
            method: 'DELETE',
            url: '/console/clusters/:clusterName'
        },
        find_clusters_batch : {
        	method : 'GET',
        	url : '/console/clusters?page=:page&size=:size',
        	isArray : true
        },
        get_clusters_count : {
        	method : 'GET',
        	url : '/console/count/clusters'
        },
        bind_dc: {
            method: 'POST',
            url: '/console/clusters/:clusterName/dcs/:dcName'
        },
        unbind_dc: {
            method: 'DELETE',
            url: '/console/clusters/:clusterName/dcs/:dcName'
        }
    });

    function getClustersCount() {
    	var d = $q.defer();
    	resource.get_clusters_count({},
    		function(result) {
    		d.resolve(result);
    	}, function(result) {
    		d.reject(result);
    	});
    	return d.promise;
    }
    
    function findClusterBatch(page,size) {
    	var d = $q.defer();
    	resource.find_clusters_batch({
    		page : page,
    		size : size
    	},
    	function(result) {
    		d.resolve(result);
    	}, function(result) {
    		d.reject(result);
    	});
    	return d.promise;
    }
    
    function findClusterDCs(clusterName) {
        var d = $q.defer();
        resource.find_cluster_dcs({
                                      clusterName: clusterName
                                  },
                                  function (result) {
                                      d.resolve(result);
                                  }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function loadCluster(clusterName) {
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

    function bindDc(clusterName, dcName) {
        var d = $q.defer();
        resource.bind_dc({
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

    function unbindDc(clusterName, dcName) {
        var d = $q.defer();
        resource.unbind_dc({
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

    function findAllClusters() {
        var d = $q.defer();
        resource.find_all_clusters({},
                                   function (result) {
                                       d.resolve(result);
                                   }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function createCluster(cluster) {
        var d = $q.defer();
        resource.create_cluster({}, cluster,
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function updateCluster(clusterName, cluster) {
        var d = $q.defer();
        resource.update_cluster({
                                  clusterName: clusterName
                              }, cluster,
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function deleteCluster(clusterName) {
        var d = $q.defer();
        resource.delete_cluster({
                                  clusterName: clusterName
                              },
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        load_cluster: loadCluster,
        findClusterDCs: findClusterDCs,
        findAllClusters: findAllClusters,
        createCluster: createCluster,
        updateCluster: updateCluster,
        deleteCluster: deleteCluster,
        findClusterBatch : findClusterBatch,
        getClustersCount : getClustersCount,
        bindDc: bindDc,
        unbindDc: unbindDc
    }
}]);
