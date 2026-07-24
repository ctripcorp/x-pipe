angular
    .module('index')
    .controller('ClusterCtl', ClusterCtl);

ClusterCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window','$interval', '$location','toastr', 'AppUtil',
'ClusterService', 'DcClusterService', 'HealthCheckService', 'ProxyService', 'ClusterType', 'ShardService'];

function ClusterCtl($rootScope, $scope, $stateParams, $window, $interval, $location, toastr, AppUtil, ClusterService,
    DcClusterService, HealthCheckService, ProxyService, ClusterType, ShardService) {

    $scope.dcs, $scope.shards;
    $scope.dcGroups = [];
    $scope.isHeteroCluster = false;
    $scope.clusterName = $stateParams.clusterName;
    $scope.routeAvail = false;

    $scope.activeDcName;
    $scope.sources=[];

    $scope.switchDc = switchDc;
    $scope.loadCluster = loadCluster;
    $scope.gotoHickwall = gotoHickwall;
    $scope.gotoHeteroHickwall = gotoHeteroHickwall;
    $scope.gotoOutComingTrafficToPeerHickwall = gotoOutComingTrafficToPeerHickwall;
    $scope.gotoInComingTrafficFromPeerHickwall = gotoInComingTrafficFromPeerHickwall;
    $scope.gotoPeerSyncFullHickwall = gotoPeerSyncFullHickwall;
    $scope.gotoPeerSyncPartialHickwall = gotoPeerSyncPartialHickwall;
    $scope.existsRoute = existsRoute;
    $scope.showCrossMasterHealthStatus = false;
    $scope.gotoCrossMasterHickwall = gotoCrossMasterHickwall;
    $scope._clusterType = ClusterType.default();
    $scope.clusterType = ClusterType.default().value;
    $scope.unfoldAllUnhealthyDelay = unfoldAllUnhealthyDelay;
    $scope.setClusterType = setClusterType;
    $scope.keeperMediumLabel = keeperMediumLabel;

    if ($scope.clusterName) {
        loadCluster();
    }

    function switchDc(dc) {
        $scope.currentDcName = dc.dcName;
        existsRoute($scope.currentDcName, $scope.clusterName);
        loadDcCluster($scope.clusterName, $scope.currentDcName);
    }

    function isTfsDiskType(diskType) {
        return diskType && diskType.toLowerCase().indexOf('tfs') === 0;
    }

    function mediumTag(diskType) {
        return isTfsDiskType(diskType) ? 'TFS' : 'BM';
    }

    function keeperMediumLabel(keeper) {
        if (!keeper || !keeper.keepercontainerDiskType) {
            return '';
        }
        return mediumTag(keeper.keepercontainerDiskType) + '/' + keeper.keepercontainerDiskType;
    }

    function loadCluster() {
        ClusterService.load_cluster($scope.clusterName)
            .then(function (cluster) {
                $scope._clusterType = ClusterType.lookup(cluster.clusterType);
                $scope.isHeteroCluster = $scope._clusterType && $scope._clusterType.useAzGroupType;
                if ($scope.isHeteroCluster) {
                    loadHeteroCluster(cluster);
                    return;
                }
                loadFlatCluster(cluster);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadHeteroCluster(cluster) {
        ClusterService.findClusterDcGroups($scope.clusterName)
            .then(function (groups) {
                if (!groups || groups.length === 0) {
                    $scope.dcGroups = [];
                    $scope.dcs = [];
                    $scope.shards = [];
                    return;
                }
                $scope.dcGroups = groups;
                $scope.dcs = flattenDcsFromGroups(groups);
                initCurrentDcName(cluster, groups);
                if (!$scope._clusterType.useAzGroupType) {
                    setClusterType(cluster.clusterType);
                }
                existsRoute($scope.currentDcName, $scope.clusterName);
                loadDcCluster($scope.clusterName, $scope.currentDcName);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadFlatCluster(cluster) {
        ClusterService.findClusterDCs($scope.clusterName)
            .then(function (result) {
                if (!result || result.length === 0) {
                    $scope.dcs = [];
                    $scope.shards = [];
                    return;
                }
                $scope.dcs = result;
                initCurrentDcName(cluster, null);
                if (!$scope._clusterType.useAzGroupType) {
                    setClusterType(cluster.clusterType);
                }
                existsRoute($scope.currentDcName, $scope.clusterName);
                loadDcCluster($scope.clusterName, $scope.currentDcName);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function initCurrentDcName(cluster, groups) {
        if ($stateParams.currentDcName) {
            var matched = findDcByName($stateParams.currentDcName, groups);
            if (matched) {
                $scope.currentDcName = matched.dcName;
                $scope.activeDcName = matched.dcName;
                return;
            }
        }
        if (groups && groups.length > 0) {
            var firstOneWayGroup = groups.find(function (group) {
                return group.azGroupClusterType === 'ONE_WAY';
            });
            if (firstOneWayGroup) {
                var activeDc = firstOneWayGroup.dcs.find(function (dc) {
                    return dc.id === firstOneWayGroup.activeAzId;
                });
                if (activeDc) {
                    $scope.currentDcName = activeDc.dcName;
                    $scope.activeDcName = activeDc.dcName;
                    return;
                }
            }
            if (firstOneWayGroup && firstOneWayGroup.dcs.length > 0) {
                $scope.currentDcName = firstOneWayGroup.dcs[0].dcName;
                return;
            }
            if (groups[0].dcs.length > 0) {
                $scope.currentDcName = groups[0].dcs[0].dcName;
                return;
            }
        }
        if ($scope.dcs && $scope.dcs.length > 0) {
            var filteredDc = $scope.dcs.filter(function (dc) {
                return dc.id === cluster.activedcId;
            });
            if (filteredDc.length > 0) {
                $scope.currentDcName = filteredDc[0].dcName;
                $scope.activeDcName = filteredDc[0].dcName;
            } else {
                $scope.currentDcName = $scope.dcs[0].dcName;
            }
        }
    }

    function findDcByName(dcName, groups) {
        if (groups && groups.length > 0) {
            for (var i = 0; i < groups.length; i++) {
                var matched = groups[i].dcs.find(function (dc) {
                    return dc.dcName === dcName;
                });
                if (matched) {
                    return matched;
                }
            }
            return null;
        }
        if (!$scope.dcs) {
            return null;
        }
        return $scope.dcs.find(function (dc) {
            return dc.dcName === dcName;
        }) || null;
    }

    function flattenDcsFromGroups(groups) {
        var seen = {};
        var flattened = [];
        groups.forEach(function (group) {
            group.dcs.forEach(function (dc) {
                if (!seen[dc.dcName]) {
                    seen[dc.dcName] = true;
                    flattened.push(dc);
                }
            });
        });
        return flattened;
    }

    function setClusterType(clusterType) {
        var type = ClusterType.lookup(clusterType);
        if (null != type) {
            $scope.clusterType = type.value;
            $scope.showCrossMasterHealthStatus = type.multiActiveDcs;
        }
    }


    function loadDcCluster(clusterName, dcName) {
        $scope.sources = [];
        $scope.shards = [];
        DcClusterService.findDcCluster(clusterName, dcName)
            .then(function (result) {
                $scope.dcCluster = result;
                if ($scope._clusterType.useAzGroupType && result.azGroupClusterType != null) {
                    setClusterType(result.azGroupClusterType)
                }

                if (result.shards != null) {
                    $scope.shards = result.shards.sort((v1, v2) => {
                        if (v1.shardTbl.shardName.length != v2.shardTbl.shardName.length)
                            return v1.shardTbl.shardName.length - v2.shardTbl.shardName.length;
                        if (v1.shardTbl.shardName > v2.shardTbl.shardName) return 1;
                        else if (v1.shardTbl.shardName < v2.shardTbl.shardName) return -1;
                        else return 0;
                    });
                    $scope.shards.forEach(function(shard) {
                        ShardService.getNodesWithAz($scope.clusterName, dcName, shard.shardTbl.shardName)
                            .then(function(redisesWithAz) {
                                var azMap = {};
                                redisesWithAz.forEach(function(r) { azMap[r.addr] = r.azName; });
                                shard.redises.forEach(function(redis) {
                                    var addr = redis.redisIp + ':' + redis.redisPort;
                                    redis.azName = azMap[addr] || null;
                                });
                                shard.keepers.forEach(function(keeper) {
                                    var addr = keeper.redisIp + ':' + keeper.redisPort;
                                    keeper.azName = azMap[addr] || null;
                                });
                            });
                    });
                }
               if (result.sources != null) {
                   result.sources.forEach(source => {
                       source.shards = source.shards.sort((v1, v2) => {
                           if (v1.shardTbl.shardName.length != v2.shardTbl.shardName.length)
                               return v1.shardTbl.shardName.length - v2.shardTbl.shardName.length;
                           if (v1.shardTbl.shardName > v2.shardTbl.shardName) return 1;
                           else if (v1.shardTbl.shardName < v2.shardTbl.shardName) return -1;
                           else return 0;
                       });
                       $scope.sources.push(source);
                   });
               }

            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

    }

    function checkCrossMasterDelay() {
        var clusterType = ClusterType.lookup($scope.clusterType)
        if (!clusterType.healthCheck) return;
        if($scope.showCrossMasterHealthStatus && $scope.shards) {
            $scope.shards.forEach(function(shard) {
                HealthCheckService.getCrossMasterDelay($scope.currentDcName, $scope.clusterName, shard.shardTbl.shardName, $scope.clusterType)
                    .then(function (result) {
                        shard.crossMasters = [];
                        for (let dc of $scope.dcs) {
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
                                redisPort: hostPort[1],
                                healthy: false
                            };

                            crossMaster.healthy = isRedisHealthy(crossMaster);
                            shard.crossMasters.push(crossMaster);
                        }
                    })
            });
        }
    }

    function healthCheck() {
        var clusterType = ClusterType.lookup($scope.clusterType)
        if (!clusterType.healthCheck) return;
        HealthCheckService.getReplDelay($scope.currentDcName, $scope.clusterName)
            .then(function(result) {
                if($scope.shards) {
                    $scope.shards.forEach(function(shard) {
                        shard.redises.forEach(function(redis) {
                            redis.delay = result.delay[redis.redisIp + ":" + redis.redisPort];
                            if (redis.master && $scope.showCrossMasterHealthStatus) {
                                redis.healthy = isRedisHealthy(redis) && isCrossMasterHealthy(shard);
                                shard.healthy = isShardHealthy(shard);
                            } else {
                                redis.healthy = isRedisHealthy(redis);
                            }
                        });
                    });
                }
            });


        if ($scope.sources) {
            $scope.sources.forEach(function (source) {
                source.shards.forEach(function (shard) {
                    HealthCheckService.getShardDelay($scope.clusterName, shard.shardTbl.shardName, shard.shardTbl.id)
                        .then(function (result) {
                            shard.delay = result.delay;
                            shard.heteroDelayhealthy = isHeteroDelayHealthy(shard);
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

    function openHickwall(promise) {
        return promise.then(function(result) {
            if(result.addr) {
                $window.open(result.addr, '_blank');
            }
        });
    }
    
    function gotoHickwall(clusterName, shardName, redisIp, redisPort, delayType) {
        openHickwall(HealthCheckService.getHickwallAddr(clusterName, shardName, redisIp, redisPort, delayType));
    }

    function gotoHeteroHickwall(clusterName, srcShardId, delayType) {
        openHickwall(HealthCheckService.getHeteroHickwallAddr(clusterName, srcShardId, delayType));
    }

    function gotoCrossMasterHickwall(shardName, destDc) {
        openHickwall(HealthCheckService.getCrossMasterHickwallAddr($scope.clusterName, shardName, $scope.currentDcName, destDc));
    }
    
    function gotoOutComingTrafficToPeerHickwall(redisIp, redisPort) {
        openHickwall(HealthCheckService.getOutComingTrafficToPeerHickwallAddr(redisIp, redisPort));
    }

    function gotoInComingTrafficFromPeerHickwall(redisIp, redisPort) {
        openHickwall(HealthCheckService.getInComingTrafficFromPeerHickwallAddr(redisIp, redisPort));
    }
    
    function gotoPeerSyncFullHickwall(redisIp, redisPort) {
        openHickwall(HealthCheckService.getPeerSyncFullHickwallAddr(redisIp, redisPort));
    }
    
    function gotoPeerSyncPartialHickwall(redisIp, redisPort) {
        openHickwall(HealthCheckService.getPeerSyncPartialHickwallAddr(redisIp, redisPort));
    }

    function existsRoute(currentDc, clusterName) {
        ProxyService.existsClusterRoute(currentDc, clusterName)
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

    function isHeteroDelayHealthy(shard) {
        if (shard.delay === null || shard.delay === undefined) return false;

        return shard.delay >= 0 && shard.delay !== 99999;
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
}
