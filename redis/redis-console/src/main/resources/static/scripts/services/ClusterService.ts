angular
    .module('services')
    .service('ClusterService', ClusterService);

ClusterService.$inject = ['$resource', '$q'];

function ClusterService($resource, $q) {
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
        find_clusters_by_names: {
            method: 'POST',
            url: '/console/clusters/by/names',
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
        reset_cluster_status: {
            method: 'POST',
            url: '/console/clusters/reset/status'
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
        get_error_migrating_clusters: {
            method: 'GET',
            url: '/console/clusters/error/migrating',
            isArray: true
        },
        get_migrating_clusters: {
            method: 'GET',
            url: '/console/clusters/migrating',
            isArray: true
        },
        find_clusters_by_dc_name_bind :{
            method: 'GET',
            url: '/console/clusters/allBind/:dcName',
            isArray: true
        },
        find_clusters_by_dc_name_bind_and_type :{
            method: 'GET',
            url: '/console/clusters/allBind/:dcName/:clusterType',
            isArray: true
        },
        find_clusters_by_dc_name:{
            method: 'GET',
            url: '/console/clusters/activeDc/:dcName',
            isArray: true
        },
        find_clusters_by_dc_name_and_type:{
            method: 'GET',
            url: '/console/clusters/activeDc/:dcName/:clusterType',
            isArray: true
        },
        find_master_unhealthy_clusters: {
            method: 'GET',
            url: '/console/clusters/master/unhealthy/:level',
            isArray: true
        },
        find_all_by_keeper_container: {
            method: 'GET',
            url: '/console/clusters/keepercontainer/:containerId',
            isArray: true
        },
        find_unhealthy_shards: {
            method: 'GET',
            url: '/console/shards/unhealthy',
            isArray: true
        },
        get_cluster_hickwall: {
            method: 'GET',
            url: '/console/cluster/hickwall/:clusterName/:clusterType'
        },
       get_cluster_default_routes_by_src_dc_name_and_cluster_name: {
            method: 'GET',
            url: '/console/clusters/:clusterName/default-routes/:srcDcName',
            isArray: true
        },
       get_cluster_used_routes_by_src_dc_name_and_cluster_name: {
            method: 'GET',
            url: '/console/clusters/:clusterName/used-routes/:srcDcName',
            isArray: true
        },
       get_cluster_designated_routes_by_src_dc_name_and_cluster_name: {
            method: 'GET',
            url: '/console/clusters/:clusterName/designated-routes/:srcDcName',
            isArray: true
        },
        update_cluster_designated_routes_by_cluster_name: {
            method: 'POST',
            url: '/console/clusters/:clusterName/designated-routes/:srcDcName',
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

    function findClustersByNames() {
        var d = $q.defer();
        resource.find_clusters_by_names(
            Array.from(arguments),
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

    function createCluster(cluster, selectedDcs, shards, dcClusters, replDirections) {
        var d = $q.defer();
        resource.create_cluster({}, {
                clusterTbl: cluster,
                dcs: selectedDcs,
                shards: shards,
                dcClusters: dcClusters,
                replDirections: replDirections
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function updateCluster(clusterName, clusterTbl, dcClusters, replDirections) {
        var d = $q.defer();
        resource.update_cluster({
                                  clusterName: clusterName
                              }, {
                                  clusterTbl : clusterTbl,
                                  dcClusters : dcClusters,
                                  replDirections : replDirections
                              },
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

    function resetClusterStatus() {
        var d = $q.defer();
        resource.reset_cluster_status(
            Array.from(arguments),
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

    function getErrorMigratingClusters() {
        var d = $q.defer();
        resource.get_error_migrating_clusters({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getMigratingClusters() {
        var d = $q.defer();
        resource.get_migrating_clusters({},
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

    function findClustersByDcNameBindAndType(dcName, clusterType) {
        var d = $q.defer();
        resource.find_clusters_by_dc_name_bind_and_type(
            {
                dcName: dcName,
                clusterType: clusterType
            },
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

    function findClustersByDcNameAndType(dcName, clusterType) {
        var d = $q.defer();
        resource.find_clusters_by_dc_name_and_type(
            {
                dcName: dcName,
                clusterType: clusterType
            },
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

    function findAllByKeeperContainer(containerId) {
        var d = $q.defer();
        resource.find_all_by_keeper_container (
            { containerId: containerId },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getUnhealthyShards() {
        var d = $q.defer();
        resource.find_unhealthy_shards ({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getClusterHickwallAddr(clusterName, clusterType) {
        var d = $q.defer();
        resource.get_cluster_hickwall ({
                 clusterName : clusterName,
                 clusterType : clusterType
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }



    function getClusterDefaultRoutesBySrcDcNameAndClusterName(srcDcName, clusterName) {
        var d = $q.defer();
        resource.get_cluster_default_routes_by_src_dc_name_and_cluster_name({
                srcDcName : srcDcName,
                clusterName : clusterName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getClusterUsedRoutesBySrcDcNameAndClusterName(srcDcName, clusterName) {
        var d = $q.defer();
        resource.get_cluster_used_routes_by_src_dc_name_and_cluster_name({
                srcDcName : srcDcName,
                clusterName : clusterName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getClusterDesignatedRoutesBySrcDcNameAndClusterName(srcDcName, clusterName) {
        var d = $q.defer();
        resource.get_cluster_designated_routes_by_src_dc_name_and_cluster_name({
                srcDcName : srcDcName,
                clusterName : clusterName
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function updateClusterDesignatedRoutes(srcDcName, clusterName, designatedRouteInfoModels) {
        var d = $q.defer();
        resource.update_cluster_designated_routes_by_cluster_name({
                srcDcName : srcDcName,
                clusterName : clusterName
             }, designatedRouteInfoModels,
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
        findClustersByNames: findClustersByNames,
        findAllClusters: findAllClusters,
        findClustersByActiveDcName: findClustersByActiveDcName,
        createCluster: createCluster,
        updateCluster: updateCluster,
        deleteCluster: deleteCluster,
        resetClusterStatus: resetClusterStatus,
        findClusterBatch : findClusterBatch,
        getClustersCount : getClustersCount,
        bindDc: bindDc,
        unbindDc: unbindDc,
        getOrganizations: getOrganizations,
        getInvolvedOrgs: getInvolvedOrgs,
        getUnhealthyClusters: getUnhealthyClusters,
        getErrorMigratingClusters: getErrorMigratingClusters,
        getMigratingClusters: getMigratingClusters,
        getUnhealthyShards: getUnhealthyShards,
        findClustersByDcNameBind: findClustersByDcNameBind,
        findClustersByDcName : findClustersByDcName,
        findClustersByDcNameBindAndType: findClustersByDcNameBindAndType,
        findClustersByDcNameAndType : findClustersByDcNameAndType,
        getMasterUnhealthyClusters : getMasterUnhealthyClusters,
        findAllByKeeperContainer: findAllByKeeperContainer,
        getClusterHickwallAddr: getClusterHickwallAddr,
        getClusterDefaultRoutesBySrcDcNameAndClusterName : getClusterDefaultRoutesBySrcDcNameAndClusterName,
        getClusterUsedRoutesBySrcDcNameAndClusterName : getClusterUsedRoutesBySrcDcNameAndClusterName,
        getClusterDesignatedRoutesBySrcDcNameAndClusterName : getClusterDesignatedRoutesBySrcDcNameAndClusterName,
        updateClusterDesignatedRoutes : updateClusterDesignatedRoutes
    }
}
