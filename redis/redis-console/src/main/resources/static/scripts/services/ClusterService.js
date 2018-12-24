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
        find_clusters_by_active_dc_name: {
            method: 'GET',
            url: '/console/clusters/all?activeDcName=:activeDcName',
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
        },
        get_all_organizations: {
            method: 'GET',
            url: '/console/organizations',
            isArray : true
        },
        get_involved_organizations: {
            method: 'GET',
            url: '/console/involved/organizations',
            isArray : true
        },
        get_unhealthy_clusters: {
            method: 'GET',
            url: '/console/clusters/unhealthy',
            isArray: true
        },
        find_clusters_by_dc_name_bind :{
            method: 'GET',
            url: '/console/clusters/allBind/:dcName',
            isArray: true
        },
        find_clusters_by_dc_name:{
            method: 'GET',
            url: '/console/clusters/activeDc/:dcName',
            isArray: true
        },
        find_master_unhealthy_clusters: {
            method: 'GET',
            url: '/console/clusters/master/unhealthy/:level',
            isArray: true
        }
    });
    function getInvolvedOrgs() {
        var d = $q.defer();
        resource.get_involved_organizations({},
            function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getOrganizations() {
        var d = $q.defer();
        resource.get_all_organizations({},
            function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });
        return d.promise;
    }
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
                              },{},
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
                              },{},
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
    
    function findClustersByActiveDcName(activeDcName) {
        var d = $q.defer();
        resource.find_clusters_by_active_dc_name(
        							  {activeDcName: activeDcName},
                                   function (result) {
                                       d.resolve(result);
                                   }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function createCluster(cluster, selectedDcs, shards) {
        var d = $q.defer();
        resource.create_cluster({}, {
        	clusterTbl : cluster,
        	slaveDcs : selectedDcs,
        	shards : shards
        	},
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

    function getUnhealthyClusters() {
        var d = $q.defer();
        resource.get_unhealthy_clusters({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findClustersByDcNameBind(dcName) {
        var d = $q.defer();
        resource.find_clusters_by_dc_name_bind(
            {dcName: dcName},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findClustersByDcName(dcName) {
        var d = $q.defer();
        resource.find_clusters_by_dc_name(
            {dcName: dcName},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getMasterUnhealthyClusters(level) {
        var d = $q.defer();
        resource.find_master_unhealthy_clusters (
            {level: level},
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
        findClustersByActiveDcName: findClustersByActiveDcName,
        createCluster: createCluster,
        updateCluster: updateCluster,
        deleteCluster: deleteCluster,
        findClusterBatch : findClusterBatch,
        getClustersCount : getClustersCount,
        bindDc: bindDc,
        unbindDc: unbindDc,
        getOrganizations: getOrganizations,
        getInvolvedOrgs: getInvolvedOrgs,
        getUnhealthyClusters: getUnhealthyClusters,
        findClustersByDcNameBind: findClustersByDcNameBind,
        findClustersByDcName : findClustersByDcName,
        getMasterUnhealthyClusters : getMasterUnhealthyClusters
    }
}]);
