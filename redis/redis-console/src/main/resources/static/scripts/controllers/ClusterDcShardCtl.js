index_module.controller('ClusterCtl', ['$rootScope', '$scope', '$stateParams', '$window','$interval', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService', 'HealthCheckService', 'ProxyService',
    function ($rootScope, $scope, $stateParams, $window, $interval, $location, toastr, AppUtil, ClusterService, ShardService, HealthCheckService, ProxyService) {

        $scope.dcs, $scope.shards;
        $scope.clusterName = $stateParams.clusterName;
        $scope.routeAvail = false;
        $scope.activeDcName;
        
        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShards = loadShards;
        $scope.gotoHickwall = gotoHickwall;
        $scope.existsRoute = existsRoute;
        
        if ($scope.clusterName) {
            loadCluster();
        }
        
        function switchDc(dc) {
            $scope.currentDcName = dc.dcName;
            existsRoute($scope.activeDcName, $scope.currentDcName);
            loadShards($scope.clusterName, dc.dcName);
        }

        function loadCluster() {
            ClusterService.findClusterDCs($scope.clusterName)
                .then(function (result) {
                    if (!result || result.length === 0) {
                        $scope.dcs = [];
                        $scope.shards = [];
                        return;
                    }
                    $scope.dcs = result;
                    
                    // TODO [marsqing] do not re-get dc data when switch dc
                    if($scope.dcs && $scope.dcs.length > 0) {
	                    $scope.dcs.forEach(function(dc){
	                    	if(dc.dcName === $stateParams.currentDcName) {
			                    $scope.currentDcName = dc.dcName;
	                    	}
	                    });
	                    
	                    if(!$scope.currentDcName) {
                            ClusterService.load_cluster($stateParams.clusterName).then(function(result) {
                                var cluster = result;
                                var filteredDc = $scope.dcs.filter(function(dc) {
                                    return dc.id === cluster.activedcId;
                                })
                                if(filteredDc.length > 0) {
                                    $scope.currentDcName = filteredDc[0].dcName;
                                    $scope.activeDcName = filteredDc[0].dcName;
                                } else {
                                    $scope.currentDcName = $scope.dcs[0].dcName; 
                                }

                                loadShards($scope.clusterName, $scope.currentDcName);
                            }, function(result) {
                                $scope.currentDcName = $scope.dcs[0].dcName; 
                                loadShards($scope.clusterName, $scope.currentDcName);
                            });
	                    } else {
                            loadShards($scope.clusterName, $scope.currentDcName);
                        }
                    }

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadShards(clusterName, dcName) {
            ShardService.findClusterDcShards(clusterName, dcName)
                .then(function (result) {
                    $scope.shards = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }
        
        function healthCheck() {
        	if($scope.shards) {
        		$scope.shards.forEach(function(shard) {
        			shard.redises.forEach(function(redis) {
        				HealthCheckService.getReplDelay(redis.redisIp, redis.redisPort)
        					.then(function(result) {
        						redis.delay = result.delay;
        					});
        			});
        		});
        	}
        }

        function gotoHickwall(clusterName, shardName, redisIp, redisPort) {
        	HealthCheckService.getHickwallAddr(clusterName, shardName, redisIp, redisPort)
        		.then(function(result) {
        			if(result.addr) {
        				$window.open(result.addr, '_blank');	
        			}
        		});
        }

        function existsRoute(activeDcName, backupDcName) {
            ProxyService.existsRouteBetween(activeDcName, backupDcName)
                .then(function (result) {
                    $scope.routeAvail = (result.state === 0);
                }, function (result) {
                    $scope.routeAvail = false;
                });
        }
        
        $scope.refreshHealthStatus = $interval(healthCheck, 2000);
        $scope.$on('$destroy', function() {
            $interval.cancel($scope.refreshHealthStatus);
          });
    }]);