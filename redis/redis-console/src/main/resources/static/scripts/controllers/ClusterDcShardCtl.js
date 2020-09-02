index_module.controller('ClusterCtl', ['$rootScope', '$scope', '$stateParams', '$window','$interval', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService', 'HealthCheckService', 'ProxyService', 'ClusterType',
    function ($rootScope, $scope, $stateParams, $window, $interval, $location, toastr, AppUtil, ClusterService, ShardService, HealthCheckService, ProxyService, ClusterType) {

        $scope.dcs, $scope.shards;
        $scope.clusterName = $stateParams.clusterName;
        $scope.routeAvail = false;
        $scope.activeDcName;
        
        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShards = loadShards;
        $scope.gotoHickwall = gotoHickwall;
        $scope.existsRoute = existsRoute;
        $scope.showCrossMasterHealthStatus = false;
        $scope.gotoCrossMasterHickwall = gotoCrossMasterHickwall;
        $scope.clusterType = ClusterType.default();
        $scope.unfoldAllUnhealthyDelay = unfoldAllUnhealthyDelay;
        
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
	                    
                        ClusterService.load_cluster($stateParams.clusterName).then(function(result) {
                            var cluster = result;
                            if (!$scope.currentDcName) {
                                var filteredDc = $scope.dcs.filter(function(dc) {
                                    return dc.id === cluster.activedcId;
                                })
                                if(filteredDc.length > 0) {
                                    $scope.currentDcName = filteredDc[0].dcName;
                                    $scope.activeDcName = filteredDc[0].dcName;
                                } else {
                                    $scope.currentDcName = $scope.dcs[0].dcName;
                                }
                            }

                            var type = ClusterType.lookup(cluster.clusterType);
                            if (null != type) {
                                $scope.clusterType = type.value;
                                $scope.showCrossMasterHealthStatus = type.multiActiveDcs;
                            }

                            loadShards($scope.clusterName, $scope.currentDcName);
                        }, function(result) {
                            $scope.currentDcName = $scope.dcs[0].dcName;
                            loadShards($scope.clusterName, $scope.currentDcName);
                        });
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

        function checkCrossMasterDelay() {
            if($scope.showCrossMasterHealthStatus && $scope.shards) {
                $scope.shards.forEach(function(shard) {
                    HealthCheckService.getCrossMasterDelay($scope.currentDcName, $scope.clusterName, shard.shardTbl.shardName, $scope.clusterType)
                        .then(function (result) {
                            shard.crossMasters = [];
                            for (dc of $scope.dcs) {
                                if (dc.dcName === $scope.currentDcName) continue;
                                if (!result || !result[dc.dcName]) {
                                    shard.crossMasters.push({
                                        redisDc: dc.dcName,
                                        delay: -1,
                                        healthy: false
                                    });
                                    continue;
                                }

                                var hostPort = Object.keys(result[dc.dcName])[0].split(":")
                                var delay = Object.values(result[dc.dcName])[0]
                                var crossMaster = {
                                    redisDc: dc.dcName,
                                    delay: delay,
                                    redisIp: hostPort[0],
                                    redisPort: hostPort[1]
                                };

                                crossMaster.healthy = isRedisHealthy(crossMaster);
                                shard.crossMasters.push(crossMaster);
                            }
                        })
                });
            }
        }
        
        function healthCheck() {
        	if($scope.shards) {
        		$scope.shards.forEach(function(shard) {
        			shard.redises.forEach(function(redis) {
        				HealthCheckService.getReplDelay(redis.redisIp, redis.redisPort, $scope.clusterType)
        					.then(function(result) {
        						redis.delay = result.delay;
        						if (redis.master && $scope.showCrossMasterHealthStatus) {
                                    redis.healthy = isRedisHealthy(redis) && isCrossMasterHealthy(shard);
                                    shard.healthy = isShardHealthy(shard);
                                } else {
                                    redis.healthy = isRedisHealthy(redis);
                                }
        					});
        			});
        		});
        	}
        }

        function isShardHealthy(shard) {
            if (!shard.redises) return true;

            for (var redis of shard.redises) {
                if (redis.master && undefined !== redis.healthy && !redis.healthy) {
                    return false
                }
            }

            return true;
        }

        function gotoHickwall(clusterName, shardName, redisIp, redisPort) {
        	HealthCheckService.getHickwallAddr(clusterName, shardName, redisIp, redisPort)
        		.then(function(result) {
        			if(result.addr) {
        				$window.open(result.addr, '_blank');	
        			}
        		});
        }

        function gotoCrossMasterHickwall(shardName, destDc) {
            HealthCheckService.getCrossMasterHickwallAddr($scope.clusterName, shardName, $scope.currentDcName, destDc)
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

        function isCrossMasterHealthy(shard) {
            if (!shard.crossMasters) return true;

            var unhealthyCrossMaster = shard.crossMasters.find(function (crossMaster) {
                return !isRedisHealthy(crossMaster);
            })
            return undefined === unhealthyCrossMaster;
        }

        function isRedisHealthy(redis) {
            if (redis.delay === null || redis.delay === undefined) return false;

            return redis.delay >= 0 && redis.delay !== 99999;
        }

        function unfoldAllUnhealthyDelay() {
            if (!$scope.showCrossMasterHealthStatus || !$scope.shards) return;

            $scope.shards.forEach(function(shard) {
                if (false === shard.healthy) {
                    shard.showCrossMasters = true;
                }
            })
        }

        $scope.refreshCrossMasterHealthStatus = $interval(checkCrossMasterDelay, 2000);
        $scope.refreshHealthStatus = $interval(healthCheck, 2000);
        $scope.$on('$destroy', function() {
            $interval.cancel($scope.refreshHealthStatus);
            $interval.cancel($scope.refreshCrossMasterHealthStatus)
          });
    }]);