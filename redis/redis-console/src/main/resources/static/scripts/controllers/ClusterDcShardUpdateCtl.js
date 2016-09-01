index_module.controller('ClusterDcShardUpdateCtl',
                        ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil',
                         'ClusterService', 'ShardService', 'RedisService', 'KeeperContainerService',
                         function ($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil,
                                   ClusterService, ShardService, RedisService, KeeperContainerService) {

                             $scope.dcs, $scope.dcActiveTab,
                                 $scope.hasMasterRedis = false, 
                                 $scope.createKeeperErrorMsg = '';
                             $scope.dcShards = {};
                             $scope.clusterName = $stateParams.clusterName;
                             $scope.shardName = $stateParams.shardName;

                             $scope.switchDc = switchDc;
                             $scope.loadCluster = loadCluster;
                             $scope.loadShard = loadShard;

                             $scope.preCreateRedis = preCreateRedis;
                             $scope.createRedis = createRedis;

                             $scope.preCreateKeeper = preCreateKeeper;
                             $scope.createKeeper = createKeeper;
                             $scope.addCreateBackupKeeperForm = addCreateBackupKeeperForm;
                             $scope.removeCreateBackupKeeperForm = removeCreateBackupKeeperForm;
                             $scope.selectKeeperContainer = selectKeeperContainer;

                             $scope.preDeleteRedis = preDeleteRedis;
                             $scope.deleteRedis = deleteRedis;

                             $scope.submitUpdates = submitUpdates;

                             if ($scope.clusterName) {
                                 loadCluster();
                             }

                             function findKeeperContainers(dcName) {
                                 KeeperContainerService.findKeeperContainersByDc(dcName)
                                     .then(function (result) {
                                         $scope.keeperContainers = result;
                                     })

                             }

                             function switchDc(dc) {
                                 $scope.currentDcName = dc.dcName;
                                 findKeeperContainers($scope.currentDcName);

                                 var shard = $scope.dcShards[$scope.currentDcName];

                                 if (!shard){
                                     loadShard($scope.clusterName, dc.dcName, $scope.shardName);
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
                                         $scope.currentDcName = $scope.dcs[0].dcName;
                                         findKeeperContainers($scope.currentDcName);
                                         loadShard($scope.clusterName, $scope.dcs[0].dcName, $scope.shardName);

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

                             function preCreateRedis() {
                                 $scope.toCreateRedis = {};

                                 $('#createRedisModal').modal('show');
                             }

                             function createRedis() {
                                 $scope.toCreateRedis.id = 0;
                                 var shard = $scope.dcShards[$scope.currentDcName];
                                 shard.redises.push($scope.toCreateRedis);
                                 $scope.toCreateRedis = {};
                                 $('#createRedisModal').modal('hide');

                                 refreshShardStatus();
                             }

                             function addCreateBackupKeeperForm() {
                                 $scope.toCreateBackupKeepers.push({});
                             }

                             function removeCreateBackupKeeperForm(index) {
                                 $scope.toCreateBackupKeepers.splice(index, 1);
                             }
                             
                             function selectKeeperContainer(selectedKeeperContainerId) {
                                 var index = indexOfKeeperContainer(selectedKeeperContainerId, $scope.freeKeeperContainers);
                                 if (index >= 0){
                                     $scope.freeKeeperContainers = _.clone($scope.freeKeeperContainers);
                                     $scope.freeKeeperContainers.splice(index, 1);
                                 }
                             }

                             function preCreateKeeper() {
                                 if (!$scope.hasActiveKeeper) {
                                     $scope.toCreateActiveKeeper = {};
                                     $scope.toCreateActiveKeeper.active = true;
                                 }

                                 // init backup container
                                 $scope.toCreateBackupKeepers = [];
                                 $scope.toCreateBackupKeepers.push({});

                                 $('#createKeeperModal').modal('show');
                             }

                             function createKeeper() {
                                 $scope.createKeeperErrorMsg = '';
                                 var shard = $scope.dcShards[$scope.currentDcName];

                                 if (!$scope.hasActiveKeeper){
                                     if (!validKeeper($scope.toCreateActiveKeeper)){
                                         $scope.createKeeperErrorMsg = "valid form content please check";
                                         return;
                                     }else {
                                         shard.keepers.push($scope.toCreateActiveKeeper);
                                     }
                                 }

                                 $scope.toCreateBackupKeepers.forEach(function (backupKeeper) {
                                     if (!validKeeper(backupKeeper)){
                                         $scope.createKeeperErrorMsg = "valid form content please check";
                                         return;
                                     }else {
                                         shard.keepers.push(backupKeeper);
                                     }
                                 });

                                 if (!$scope.createKeeperErrorMsg){
                                     $('#createKeeperModal').modal('hide');
                                     refreshShardStatus();
                                 }

                             }

                             function validKeeper(keeper) {
                                 return keeper && keeper.ip && keeper.port;
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

                             function submitUpdates() {

                                 var shard = $scope.dcShards[$scope.currentDcName];

                                 //pre check
                                 //must have and only have one active keeper
                                 var activeKeeperCnt = 0;
                                 shard.keepers.forEach(function (keeper) {
                                     if (keeper.active){
                                         activeKeeperCnt ++;
                                     }
                                 });
                                 if (activeKeeperCnt != 1){
                                    toastr.error("must have and only have one active keeper");
                                     return;
                                 }

                                 RedisService.updateShardRedis($scope.clusterName, $scope.currentDcName, shard.id, shard)
                                     .then(function (result) {
                                         toastr.success("operator success");
                                     }, function (result) {
                                         toastr.error(AppUtil.errorMsg(result), "operator fail");
                                     });

                             }

                             function refreshShardStatus() {
                                 $scope.hasRedisMaster = false;

                                 var shard = $scope.dcShards[$scope.currentDcName];

                                 if (shard.redises && shard.redises.length) {
                                     shard.redises.forEach(function (redis) {
                                         if (redis.master == "" || redis.master == "true") {
                                             $scope.hasRedisMaster = true;
                                             redis.master = "true";
                                         } else {
                                             redis.master = "false";
                                         }

                                     })
                                 }

                                 $scope.freeKeeperContainers = $scope.keeperContainers;
                                 $scope.hasActiveKeeper = false;
                                 if (shard.keepers && shard.keepers.length > 0){
                                     shard.keepers.forEach(function (keeper) {
                                         var index = indexOfKeeperContainer(keeper.keeperContainerId, $scope.keeperContainers);
                                         if (index >= 0){
                                             $scope.freeKeeperContainers.splice(index, 1);
                                         }
                                         if (keeper.active){
                                             $scope.hasActiveKeeper = true;
                                         }
                                     })
                                 }

                             }

                             function indexOfKeeperContainer(keeperContainerId, keeperContainers) {
                                 var index = -1;
                                 keeperContainers.forEach(function (keeperContainer, i) {
                                     if (keeperContainer.keepercontainerId == keeperContainerId){
                                         index = i;
                                         return;
                                     }
                                 })

                                 return index;
                             }


                         }]);
