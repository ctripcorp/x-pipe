angular
    .module('index')
    .controller('ClusterDcShardUpdateCtl', ClusterDcShardUpdateCtl);

ClusterDcShardUpdateCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil',
    'ClusterService', 'DcClusterService', 'ShardService', 'RedisService', 'ApplierService',
    'AppliercontainerService', 'ReplDirectionService', 'KeeperContainerService', 'ClusterType'];

function ClusterDcShardUpdateCtl($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService,
        DcClusterService, ShardService, RedisService, ApplierService, AppliercontainerService,
        ReplDirectionService, KeeperContainerService, ClusterType) {

    $scope.dcs,$scope.hasMasterRedis = false, $scope.createKeeperErrorMsg = '';
    $scope.createApplierErrorMsg = '';
    $scope.dcShards = {};
    $scope.dcSourceShards = {};
    $scope.replDirection = {};
    $scope.clusterName = $stateParams.clusterName;
    $scope.shardName = $stateParams.shardName;
    $scope.currentDcName = $stateParams.currentDcName;
    $scope.srcDcName = $stateParams.srcDcName;

    $scope.masterDcName;

    $scope.switchDc = switchDc;
    $scope.loadCluster = loadCluster;
    $scope.loadShard = loadShard;
    $scope.loadSourceShard = loadSourceShard;

    $scope.preCreateRedis = preCreateRedis;
    $scope.createRedis = createRedis;

    $scope.preCreateKeeper = preCreateKeeper;
    $scope.createKeeper = createKeeper;
    $scope.addCreateBackupKeeperForm = addCreateBackupKeeperForm;
    $scope.removeCreateBackupKeeperForm = removeCreateBackupKeeperForm;

    $scope.preDeleteRedis = preDeleteRedis;
    $scope.deleteRedis = deleteRedis;

    $scope.preCreateApplier = preCreateApplier;
    $scope.createApplier = createApplier;
    $scope.preDeleteApplier = preDeleteApplier;
    $scope.deleteApplier = deleteApplier;

    $scope.addCreateBackupApplierForm = addCreateBackupApplierForm;
    $scope.removeCreateBackupApplierForm = removeCreateBackupApplierForm;


    $scope.submitUpdates = submitUpdates;

    $scope.hasRedisMaster = false;
    $scope.useKeeper = false;
    $scope.multiActiveDcs = false;
    $scope.isSource = false;

    if ($scope.srcDcName) {
        $scope.isSource = true;
    }

    if ($scope.clusterName) {
        loadCluster();
    }

    function findActiveKeeperContainersByCluster(dcName, clusterName) {
        KeeperContainerService.findAvailableKeepersByDcAndCluster(dcName, clusterName)
           .then(function(result) {
                result.sort(function(keeperA, keeperB) {
                    return keeperA.count - keeperB.count;
                });
                   $scope.keeperContainers = result;
               })
    }

    function findActiveApplierContainersByCluster(dcName, clusterName) {
        AppliercontainerService.getActiveAppliercontainersByDcCluster(dcName, clusterName)
            .then(function(result) {
                result.sort(function(applierA, applierB) {
                    return applierA.count - applierB.count;
                });

                $scope.appliercontainers = result;
            });
    }

    function switchDc(dc) {
        $scope.currentDcName = dc.dcName;
        if ($scope.useKeeper) findActiveKeeperContainersByCluster($scope.currentDcName, $scope.clusterName);

        if ($scope.isSource) {
            loadReplDirection($scope.clusterName, $scope.srcDcName, $scope.currentDcName);
            if ($scope.supportApplier) findActiveApplierContainersByCluster($scope.currentDcName, $scope.clusterName);
            var sourceShard = $scope.dcSourceShards[$scope.currentDcName];
            if (!sourceShard) {
                loadSourceShard($scope.clusterName, $scope.srcDcName, $scope.currentDcName, $scope.shardName);
            }
        } else {
            var shard = $scope.dcShards[$scope.currentDcName];

            if (!shard){
                loadShard($scope.clusterName, dc.dcName, $scope.shardName);
            } else {
                refreshShardStatus();
            }
        }

    }

    function loadCluster() {
        ClusterService.findClusterDCs($scope.clusterName)
            .then(function (result) {
                if (!result || result.length == 0) {
                    $scope.dcs = [];
                    return;
                }
                $scope.dcs = result;

                ClusterService.load_cluster($scope.clusterName)
        	 		.then(function(result) {
        	 			var clusterType = ClusterType.lookup(result.clusterType);
        	 			$scope.cluster = result;
        	 			$scope.useKeeper = clusterType && clusterType.useKeeper;
        	 			$scope.supportApplier = clusterType && clusterType.supportApplier;
                        $scope.multiActiveDcs = clusterType && clusterType.multiActiveDcs;
        	 			for(var i = 0 ; i != $scope.dcs.length; ++i) {
        	 				if($scope.dcs[i].id === $scope.cluster.activedcId) {
        	 					$scope.masterDcName = $scope.dcs[i].dcName;
        	 					break;
        	 				}
                       }
        	 		}, function(result) {
        	 			toastr.error(AppUtil.errorMsg(result));
        	 		});

                if(!$scope.currentDcName) $scope.currentDcName = $scope.dcs[0].dcName;
                findActiveKeeperContainersByCluster($scope.currentDcName, $scope.clusterName);
                findActiveApplierContainersByCluster($scope.currentDcName, $scope.clusterName);

                if ($scope.isSource) {
                    loadReplDirection($scope.clusterName, $scope.srcDcName, $scope.currentDcName);
                    loadSourceShard($scope.clusterName, $scope.srcDcName, $scope.currentDcName, $scope.shardName);
                } else {
                    loadShard($scope.clusterName, $scope.currentDcName, $scope.shardName);
                }

            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadShard(clusterName, dcName, shardName) {
        ShardService.findClusterDcShard(clusterName, dcName, shardName)
            .then(function (result) {
                $scope.dcShards[dcName] = result;

                refreshShardStatus();

            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadSourceShard(clusterName, srcDcName, toDcName, shardName) {
        ShardService.findClusterDcSourceShard(clusterName, srcDcName, toDcName, shardName)
            .then(function (result) {
                $scope.dcSourceShards[toDcName] = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadReplDirection(clusterName, srcDcName, toDcName) {
        ReplDirectionService.findReplDirectionByClusterAndSrcToDc(clusterName, srcDcName, toDcName)
            .then(function (result) {
                $scope.replDirection = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function preCreateRedis() {
        $scope.toCreateRedis = {
       		 redisPort : 6379,
       		 master : false
        };

        $('#createRedisModel').modal('show');
    }

    function createRedis() {
        var shard = $scope.dcShards[$scope.currentDcName];
        shard.redises.push($scope.toCreateRedis);
        $scope.toCreateRedis = {};
        $('#createRedisModel').modal('hide');

        refreshShardStatus();
    }

    function addCreateBackupKeeperForm() {
        $scope.toCreateOtherKeepers.push({});
    }

    function removeCreateBackupKeeperForm(index) {
        $scope.toCreateOtherKeepers.splice(index, 1);
    }

    function preCreateKeeper() {
        $scope.orgSpecifiedKeepers = [];
        $scope.toCreateFirstKeeper = {
        };
        // init backup container
        $scope.toCreateOtherKeepers = [];

        var shard = $scope.dcShards[$scope.currentDcName];
         KeeperContainerService.findAvailableKeepersByDc($scope.currentDcName, shard).then(function(keepers){
            var keeper = keepers.shift();
            if(keeper){
                $scope.toCreateFirstKeeper = {
                   keepercontainerId : keeper.keepercontainerId.toString(),
                   redisPort : keeper.redisPort
                };
            }

            while(keeper = keepers.shift()){
               $scope.toCreateOtherKeepers.push({
                   keepercontainerId : keeper.keepercontainerId.toString(),
                   redisPort : keeper.redisPort

                });
            }
         });

        $('#createKeeperModel').modal('show');
    }

    function createKeeper() {
        $scope.createKeeperErrorMsg = '';
        var shard = $scope.dcShards[$scope.currentDcName];

            if (!validKeeper($scope.toCreateFirstKeeper)){
                $scope.createKeeperErrorMsg = "valid form content please check";
                return;
            }else {
           	 var keeperContainerId = $scope.toCreateFirstKeeper.keepercontainerId;
           	 for(var i = 0 ; i != $scope.keeperContainers.length; ++i) {
           		 if($scope.keeperContainers[i].keepercontainerId === parseInt(keeperContainerId)) {
           			 $scope.toCreateFirstKeeper.redisIp = $scope.keeperContainers[i].keepercontainerIp;
           			 break;
           		 }
           	 }
                shard.keepers.push($scope.toCreateFirstKeeper);
            }

        $scope.toCreateOtherKeepers.forEach(function (otherKeeper) {
            if (!validKeeper(otherKeeper)){
                $scope.createKeeperErrorMsg = "valid form content please check";
                return;
            }else {
           	 var keeperContainerId = otherKeeper.keepercontainerId;
           	 for(var i = 0 ; i != $scope.keeperContainers.length; ++i) {
           		 if($scope.keeperContainers[i].keepercontainerId === parseInt(keeperContainerId)) {
           			 otherKeeper.redisIp = $scope.keeperContainers[i].keepercontainerIp;
           			 break;
           		 }
           	 }
                shard.keepers.push(otherKeeper);
            }
        });

        if (!$scope.createKeeperErrorMsg){
            $('#createKeeperModel').modal('hide');
            refreshShardStatus();
        }

    }

    function validKeeper(keeper) {
        return keeper && keeper.redisPort;
    }

    function preDeleteRedis(redis) {
        $scope.toDeleteRedis = {};
        $scope.toDeleteRedis = redis;
        $('#deleteRedisConfirm').modal('show');
    }

    function deleteRedis() {
        var shard = $scope.dcShards[$scope.currentDcName];
        var index = -1;
        for (var cnt_redis = 0; cnt_redis != shard.redises.length; ++cnt_redis) {
            if ($scope.toDeleteRedis == shard.redises[cnt_redis]) {
                index = cnt_redis;
                break;
            }
        }
        if (index != -1) {
            shard.redises.splice(index, 1);
            $scope.toDeleteRedis = {};
            refreshShardStatus();
            return;
        }

        for (var cnt_keeper = 0; cnt_keeper != shard.keepers.length; ++cnt_keeper) {
            if ($scope.toDeleteRedis == shard.keepers[cnt_keeper]) {
                index = cnt_keeper;
                break;
            }
        }
        if (index != -1) {
            shard.keepers.splice(index, 1);
            $scope.toDeleteRedis = {};
            refreshShardStatus();
            return;
        }

    }

    function preCreateApplier() {
        $scope.orgSpecifiedAppliers = [];
        $scope.toCreateFirstApplier = {};
        $scope.toCreateOtherAppliers = [];

        var sourceShard = $scope.dcSourceShards[$scope.currentDcName];

        ApplierService.findAvailableAppliersByDc($scope.currentDcName, sourceShard)
            .then(function(appliers) {
                var applier = appliers.shift();
                if (applier) {
                    $scope.toCreateFirstApplier = {
                        containerId : applier.containerId.toString(),
                        port : applier.port
                    };
                }

                while (applier = appliers.shift()) {
                    $scope.toCreateOtherAppliers.push( {
                        containerId : applier.containerId.toString(),
                        port : applier.port
                    });
                }
            });

        $('#createApplierModel').modal('show');
    }

    function createApplier() {
        $scope.createApplierErrorMsg = '';
        var sourceShard = $scope.dcSourceShards[$scope.currentDcName];

        if (!validApplier($scope.toCreateFirstApplier)){
            $scope.createApplierErrorMsg = "valid form content please check";
            return;
        }else {
             var appliercontainerId = $scope.toCreateFirstApplier.containerId;
             for(var i = 0 ; i != $scope.appliercontainers.length; ++i) {
                 if($scope.appliercontainers[i].appliercontainerId === parseInt(appliercontainerId)) {
                     $scope.toCreateFirstApplier.ip = $scope.appliercontainers[i].appliercontainerIp;
                     break;
                 }
             }
            sourceShard.appliers.push($scope.toCreateFirstApplier);
        }

        $scope.toCreateOtherAppliers.forEach(function (otherApplier) {
            if (!validApplier(otherApplier)){
                $scope.createApplierErrorMsg = "valid form content please check";
                return;
            }else {
                 var appliercontainerId = otherApplier.containerId;
                 for(var i = 0 ; i != $scope.appliercontainers.length; ++i) {
                     if($scope.appliercontainers[i].appliercontainerId === parseInt(appliercontainerId)) {
                         otherApplier.ip = $scope.appliercontainers[i].appliercontainerIp;
                         break;
                     }
                 }
                sourceShard.appliers.push(otherApplier);
            }
        });

        if (!$scope.createApplierErrorMsg){
            $('#createApplierModel').modal('hide');
        }
    }

    function validApplier(applier) {
        return applier && applier.port;
    }

    function addCreateBackupApplierForm() {
        $scope.toCreateOtherAppliers.push({});
    }

    function removeCreateBackupApplierForm(index) {
        $scope.toCreateOtherAppliers.splice(index, 1);
    }

    function preDeleteApplier(applier) {
        $scope.toDeleteApplier = {};
        $scope.toDeleteApplier = applier;
        $('#deleteApplierConfirm').modal('show');
    }

    function deleteApplier() {
        var source = $scope.dcSourceShards[$scope.currentDcName];
        var index = -1;
        for (var cnt_applier = 0; cnt_applier != source.appliers.length; ++cnt_applier) {
            if ($scope.toDeleteApplier == source.appliers[cnt_applier]) {
                index = cnt_applier;
                break;
            }
        }
        if (index != -1) {
            source.appliers.splice(index, 1);
            $scope.toDeleteApplier = {};
            return;
        }
    }

    function submitUpdates() {

        if ($scope.isSource) {
            var sourceShard = $scope.dcSourceShards[$scope.currentDcName];
            ApplierService.updateAppliers($scope.currentDcName, $scope.clusterName, sourceShard.shardTbl.shardName, $scope.replDirection.id, sourceShard)
                .then(function (result) {
                    if(result.message == 'success' ) {
                    toastr.success("operation success");
                    $window.location.href =
                        "#/cluster_dc_shards/" + $scope.clusterName + "/" + $scope.currentDcName;
                    } else {
                        toastr.error(result.message, "operation fail");
                    }
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), "operation fail");
                });
        } else {
            var shard = $scope.dcShards[$scope.currentDcName];

            RedisService.updateShardRedis($scope.clusterName, $scope.currentDcName, shard.shardTbl.shardName, shard)
                .then(function (result) {
                    toastr.success("operation success");
                    $window.location.href =
                        "#/cluster_dc_shards/" + $scope.clusterName + "/" + $scope.currentDcName;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), "operation fail");
                });
        }
    }

    function refreshShardStatus() {
        if(!$scope.multiActiveDcs && !$scope.currentDcName === $scope.masterDcName) {
       	 $scope.hasRedisMaster = false;
       	 return;
        }

        var shard = $scope.dcShards[$scope.currentDcName];
        $scope.hasRedisMaster = false;
        if (shard.redises && shard.redises.length) {
            shard.redises.forEach(function (redis) {
                if (redis.master === true) {
                    $scope.hasRedisMaster = true;
                }
            })
        }
    }
}
