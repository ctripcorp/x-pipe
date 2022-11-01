angular
    .module('index')
    .controller('ClusterFromCtl', ClusterFromCtl);

ClusterFromCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
      'DcService', 'DcClusterService', 'ReplDirectionService', 'SentinelService', 'ClusterType'];

function ClusterFromCtl($rootScope, $scope, $stateParams, $window, toastr, AppUtil,
    ClusterService, DcService, DcClusterService, ReplDirectionService, SentinelService, ClusterType) {

    $rootScope.currentNav = '1-3';

    var OPERATE_TYPE = {
        CREATE: 'create',
        UPDATE: 'update',
        RETRIEVE: 'retrieve'
    };

    var clusterName = $stateParams.clusterName;

    $scope.operateType = $stateParams.type;
    $scope.allDcs = [];
    $scope.selectedDcs = [];
    $scope.shards = [];
    $scope.currentShard = {};
    $scope.sentinels = {};
    $scope.organizations = [];
    $scope.organizationNames = [];
    $scope.clusterTypeName = undefined;
    $scope.clusterRelatedDcNames = [];

    $scope.dcClusterModels = [];
    $scope.toCreateDcGroups = [];
    $scope.groupTypes = ['MASTER', 'DR_MASTER'];
    $scope.groupNames = {};
    $scope.allDcNames = [];
    $scope.toUpdateDcGroup = [];
    $scope.toCreateReplDirections = [];
    $scope.replDirections = [];
    $scope.updateReplDirectionIndex = 0;
    $scope.toUpdateReplDirection = [];
    $scope.drMasterShards = [];
    $scope.masterShards = [];
    $scope.masterShardNum = {};
    $scope.drMasterDcs = [];
    $scope.activeDcName = '';
    $scope.isHeteroCluster = false;

    $scope.doCluster = doCluster;
    $scope.getDcName = getDcName;
    $scope.preDeleteCluster = preDeleteCluster;
    $scope.deleteCluster = deleteCluster;
    $scope.slaveExists = slaveExists;
    $scope.toggle = toggle;
    $scope.preCreateShard = preCreateShard;
    $scope.createShard = createShard;
    $scope.deleteShard = deleteShard;
    $scope.activeDcSelected = activeDcSelected;
    $scope.shardNameChanged = shardNameChanged;

    $scope.showActiveDc = true
    $scope.clusterTypes = ClusterType.values()
    $scope.selectedType = ClusterType.default().value
    $scope.typeChange = typeChange;

    $scope.activeDcNameChange = activeDcNameChange;
    $scope.toCreateDcGroupNameChange = toCreateDcGroupNameChange;
    $scope.preCreateDcGroup = preCreateDcGroup;
    $scope.createDcGroup = createDcGroup;
    $scope.preDeleteDcGroup = preDeleteDcGroup;
    $scope.deleteDcGroup = deleteDcGroup;
    $scope.preUpdateDcGroup = preUpdateDcGroup;
    $scope.confirmUpdateDcGroup = confirmUpdateDcGroup;
    $scope.updateDcGroup = updateDcGroup;
    $scope.removeToCreateDcGroups = removeToCreateDcGroups;
    $scope.addOtherDcGroup = addOtherDcGroup;
    $scope.preUpdateDrMasterShardModel = preUpdateDrMasterShardModel;
    $scope.preUpdateMasterShardModel = preUpdateMasterShardModel;
    $scope.confirmUpdateShardModel = confirmUpdateShardModel;
    $scope.updateShardModel = updateShardModel;

    $scope.preCreateReplDirection = preCreateReplDirection;
    $scope.createReplDirection = createReplDirection;
    $scope.confirmDeleteReplDirection = confirmDeleteReplDirection;
    $scope.deleteReplDirection = deleteReplDirection;
    $scope.removeToCreateReplDirections = removeToCreateReplDirections;
    $scope.addOtherReplDirection = addOtherReplDirection;
    $scope.preUpdateReplDirection = preUpdateReplDirection;
    $scope.confirmUpdateReplDirection = confirmUpdateReplDirection;

    $scope.changeIsHeteroCluster = changeIsHeteroCluster;
    $scope.changeSymmetryToHeteroCluster = changeSymmetryToHeteroCluster;
    $scope.changeHeteroToSymmetryCluster = changeHeteroToSymmetryCluster;

    init();

    function init() {

       DcService.loadAllDcs()
           .then(function (result) {
               $scope.allDcs = result;
               $scope.allDcs.forEach(function(dc) {
                    var dcGroupNames = [dc.dcName, 'SHA-CTRIP-PRIV'];
                    $scope.groupNames[dc.dcName] = dcGroupNames;
               	    SentinelService.findSentinelsByDc(dc.dcName)
                        .then(function(result) {
                            $scope.sentinels[dc.dcName] = result;
                        });
               });

               $scope.allDcNames = result.map(function (dc) {
                   return dc.dcName;
               });

               if ($scope.operateType != OPERATE_TYPE.CREATE) {
                    loadCluster(clusterName);
               } else {
                   $scope.cluster = {};
                   $scope.clusterRelatedDcs = [];
               }
           });

       ClusterService.getOrganizations()
       .then(function (result) {
            $scope.organizations = result;
           $scope.organizationNames = result.map(function (org) {
               return org.orgName;
           });
            console.log($scope.organizationNames);
        });

    }

    function loadCluster(clusterName) {
        ClusterService.load_cluster(clusterName)
            .then(function (result) {
                $scope.cluster = result;
                var clusterType = ClusterType.lookup(result.clusterType)
                $scope.clusterTypeName = clusterType.name
                $scope.selectedType = clusterType.value
                $scope.showActiveDc = !clusterType.multiActiveDcs
                $scope.activeDcName = getDcName(result.activedcId);
                $scope.drMasterDcs.push($scope.activeDcName);

                loadAllDcClusters(clusterName);
                loadAllReplDirections(clusterName);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function loadAllDcClusters(clusterName) {
        DcClusterService.findDcClusterByCluster(clusterName)
            .then(function (result) {
                $scope.dcClusterModels = result;
                $scope.test = result;
                $scope.dcClusterModels.forEach(function(dcClusterModel){
                    $scope.clusterRelatedDcNames.push(dcClusterModel.dc.dc_name);
                    if (isDrMasterGroup(dcClusterModel.dcCluster.groupType)) {
                        dcClusterModel.shardNum = dcClusterModel.shards.length;
                        if (dcClusterModel.dc.dc_name == $scope.activeDcName) {
                            $scope.drMasterShards=[];
                            dcClusterModel.shards.forEach(function(shard) {
                                $scope.drMasterShards.push(shard.shardTbl);
                                $scope.shards.push(shard.shardTbl);
                            });

                        }
                    } else if (isMasterGroup(dcClusterModel.dcCluster.groupType)) {
                        if ($scope.selectedType == 'one_way'){
                            $scope.isHeteroCluster = true;
                        }
                        $scope.masterShards[dcClusterModel.dcCluster.groupName] = [];
                        dcClusterModel.shardNum = dcClusterModel.shards.length;
                        var index = 0;
                        dcClusterModel.shards.forEach(function(shard){
                            shard.shardTbl.shardGroup = dcClusterModel.dcCluster.groupName;
                            shard.shardTbl.groupIndex = index++;
                            $scope.masterShards[dcClusterModel.dcCluster.groupName].push(shard.shardTbl);
                            $scope.shards.push(shard.shardTbl);
                        });
                    }
                });
                updateAllMasterShards();
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function isDrMasterGroup(groupType) {
        if ((groupType == null && $scope.selectedType == 'one_way') || groupType == $scope.groupTypes[1]) {
            return true;
        }

        return false;
    }

    function isMasterGroup(groupType) {
        if (groupType == $scope.groupTypes[0]) {
            return true;
        }
        return false;
    }

    function loadAllReplDirections(clusterName) {
        ReplDirectionService.findReplDirectionByCluster(clusterName)
            .then(function (result) {
                $scope.replDirections = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function doCluster() {
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            if ($scope.isHeteroCluster) {
                 $scope.clusterRelatedDcs = [];
                 $scope.shards = [];

                 $scope.dcClusterModels.forEach(function (dcClusterModel) {
                    addShardToDcModel(dcClusterModel);
                 });

                 $scope.replDirections.forEach(function(replDirection) {
                    replDirection.clusterName = $scope.cluster.clusterName;
                 });

                 if ($scope.activeDcName != '' && $scope.isHeteroCluster) {
                    var dcId = getDcId($scope.activeDcName);
                    if (dcId == -1) {
                        toastr.error("activeDcName" + $scope.activeDcName + "is not exist", "创建失败");
                    }
                    $scope.cluster.activedcId = getDcId($scope.activeDcName);
                 }
            } else {
                 $scope.shards.forEach(function(shard) {
                    shard.shardTbl = {};
                    shard.shardTbl.shardName = shard.shardName;
                    shard.shardTbl.setinelMonitorName = shard.setinelMonitorName;
                 });
                 $scope.replDirections = [];
                 $scope.dcClusterModels = [];
            }

            $scope.cluster.clusterType = $scope.selectedType;
            ClusterService.createCluster($scope.cluster, $scope.clusterRelatedDcs, $scope.shards, $scope.dcClusterModels, $scope.replDirections)
                .then(function (result) {
                    toastr.success("创建成功");
                    $window.location.href =
                        "/#/cluster_form?clusterName=" + $scope.cluster.clusterName+ "&type=retrieve";
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), "创建失败");
                });
        } else {
            if ($scope.isHeteroCluster) {
                $scope.dcClusterModels.forEach(function (dcClusterModel) {
                   if (dcClusterModel.dcCluster.clusterId == undefined) {
                        dcClusterModel.dcCluster.clusterId = $scope.cluster.id;
                   }
                   if (dcClusterModel.dcCluster.dcId == undefined) {
                        dcClusterModel.dcCluster.dcId = getDcId(dcClusterModel.dc.dc_name);
                   }
                   addShardToDcModel(dcClusterModel);
                });
            }

            ClusterService.updateCluster($scope.cluster.clusterName, $scope.cluster, $scope.dcClusterModels, $scope.replDirections)
                .then(function (result) {
                    toastr.success("更新成功");
                    $window.location.href =
                        "/#/cluster_form?clusterName=" + $scope.cluster.clusterName + "&type=retrieve";
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), "更新失败");
                });
        }

    }

    function addShardToDcModel(dcClusterModel) {
       if (isDrMasterGroup(dcClusterModel.dcCluster.groupType)){
           dcClusterModel.shards = [];
           $scope.drMasterShards.forEach(function(shard){
               dcClusterModel.shards.push({
                   "shardTbl" : {
                       "shardName" : shard.shardName,
                       "setinelMonitorName" : shard.setinelMonitorName
                   }
               })
           });
       } else if (isMasterGroup(dcClusterModel.dcCluster.groupType)){
           dcClusterModel.shards = [];
           $scope.masterShards[dcClusterModel.dcCluster.groupName].forEach(function(shard){
              dcClusterModel.shards.push({
                  "shardTbl" : {
                      "shardName" : shard.shardName,
                      "setinelMonitorName" : shard.setinelMonitorName
                  }
              })
          });
       }
    }

    function getDcName(dcId) {
        var result = '';
        $scope.allDcs.forEach(function (dc) {
            if (dc.id == dcId){
                result = dc.dcName;
                return;
            }
        });
        return result;
    }

    function getDcId(dcName) {
        var result = -1;
        $scope.allDcs.forEach(function (dc) {
            if (dc.dcName == dcName){
                result = dc.id;
                return;
            }
        });
        return result;
    }

    function preDeleteCluster() {
        $('#deleteClusterConfirm').modal('show');

    }
    function deleteCluster(cluster) {
        ClusterService.deleteCluster($scope.cluster.clusterName)
            .then(function (result) {
                $('#deleteClusterConfirm').modal('hide');
                toastr.success('删除成功');
                setTimeout(function () {
                    $window.location.href = '/#/cluster_list';
                },1000);


            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '删除失败');
            })
    }

    function slaveExists(dc) {
   	 return $scope.selectedDcs.indexOf(dc) > -1;
    }

    function toggle(dc) {
   	var idx = $scope.selectedDcs.indexOf(dc);
	    if (idx > -1) {
	    	$scope.selectedDcs.splice(idx, 1);
	    	var clusterRelatedDcIdx = $scope.clusterRelatedDcs.indexOf(dc);
	    	if(clusterRelatedDcIdx > -1) {
	    		$scope.clusterRelatedDcs.splice(clusterRelatedDcIdx,1);
	    	}
	    }
	    else {
	    	$scope.selectedDcs.push(dc);
	    	$scope.clusterRelatedDcs.push(dc);
	    }
    }

    function preCreateShard() {
        $('#createShardModal').modal('show');
    }

    function createShard() {
        var shardSentinels = {};
        for(var key in $scope.currentShard.sentinels) {
       	 shardSentinels[key] = $scope.currentShard.sentinels[key];
        }
    	$scope.shards.push({
    		shardName: $scope.currentShard.shardName,
    		setinelMonitorName: $scope.currentShard.setinelMonitorName,
           sentinels : shardSentinels
    	});
    	$('#createShardModal').modal('hide');
    }

    function deleteShard(shardName) {
    	for(var i in $scope.shards) {
    		if($scope.shards[i].shardName == shardName) {
    			$scope.shards.splice(i, 1);
    			break;
    		}
    	}
    }

    function activeDcNameChange() {
        $scope.replDirections = [];
        $scope.clusterRelatedDcNames.forEach(function(dcName) {
            if (dcName !== $scope.activeDcName) {
                $scope.replDirections.push({
                    "clusterName": $scope.cluster.clusterName,
                    "srcDcName": $scope.activeDcName,
                    "fromDcName": $scope.activeDcName,
                    "toDcName": dcName,
              });
            }
        });
    }

    function activeDcSelected(toPush) {
   	 var forDeleted = [];
   	 $scope.clusterRelatedDcs.forEach(function(dc) {
   		if($scope.selectedDcs.indexOf(dc) == -1) {
   			forDeleted.push(dc);
   		}
   	 });

   	 forDeleted.forEach(function(forDeleted) {
   		var idx = $scope.clusterRelatedDcs.indexOf(forDeleted);
   		if(idx > -1) {
   			$scope.clusterRelatedDcs.splice(idx,1);
   		}
   	 });

   	 $scope.clusterRelatedDcs.push(toPush);
    }

    function shardNameChanged() {
   	 if ($scope.cluster) {
   	     if (!$scope.isHeteroCluster && $scope.currentShard) {
   		     if ($scope.currentShard.shardName.indexOf($scope.cluster.clusterName) >=0 ){
   			    $scope.currentShard.setinelMonitorName = $scope.currentShard.shardName;
   			 } else {
   			    $scope.currentShard.setinelMonitorName = $scope.cluster.clusterName + $scope.currentShard.shardName;
   			 }
   	     }
   		 if ($scope.isHeteroCluster && $scope.toUpdateShardModel) {
   		     if ($scope.toUpdateShardModel.shardName.indexOf($scope.cluster.clusterName) >=0 ){
   			    $scope.toUpdateShardModel.setinelMonitorName = $scope.toUpdateShardModel.shardName;
   			 } else {
   			    $scope.toUpdateShardModel.setinelMonitorName = $scope.cluster.clusterName + $scope.toUpdateShardModel.shardName;
   			 }
   		 }
   	 }
    }

    function typeChange() {
        $scope.showActiveDc = !ClusterType.lookup($scope.selectedType).multiActiveDcs
        if (!$scope.showActiveDc) {
            var activeDc = $scope.allDcs.find(dc => dc.id === $scope.cluster.activedcId)
            var clusterRelatedDcIdx = $scope.clusterRelatedDcs.indexOf(activeDc);
            var selectedDcIdx = $scope.selectedDcs.indexOf(activeDc)
            if(clusterRelatedDcIdx > -1) {
                $scope.clusterRelatedDcs.splice(clusterRelatedDcIdx,1);
            }
            if (selectedDcIdx > -1) {
                $scope.selectedDcs.splice(selectedDcIdx, 1)
            }
            $scope.cluster.activedcId = undefined
        }
        if ($scope.isHeteroCluster) {
            clearHeteroInfo();
        }
        $scope.isHeteroCluster = false;
    }

    function preCreateDcGroup() {
        $scope.toCreateDcGroups=[];
        $scope.toCreateDcGroups.push({
            "dc":  {
                "dc_name":$scope.allDcNames[0]
            },
            "dcCluster" : {
                "groupName": $scope.allDcNames[0],
                "groupType": $scope.groupTypes[1]
            },
            "shardNum": 0
        })
        $('#createDcGroupModal').modal('show');
    }

    function createDcGroup() {
        $scope.toCreateDcGroups.forEach(function(toCreateDcGroup){
            if (!isAlreadyExistDcGroup(toCreateDcGroup.dc.dc_name)) {
                $scope.dcClusterModels.push(toCreateDcGroup);
                $scope.clusterRelatedDcNames.push(toCreateDcGroup.dc.dc_name);
                if (isDrMasterGroup(toCreateDcGroup.dcCluster.groupType)) {
                    $scope.drMasterDcs.push(toCreateDcGroup.dc.dc_name);
                    $scope.drMasterShardNum = toCreateDcGroup.shardNum;
                    updateDrMasterShard();
                } else if (isMasterGroup(toCreateDcGroup.dcCluster.groupType)){
                    $scope.masterShardNum[toCreateDcGroup.dcCluster.groupName] = toCreateDcGroup.shardNum;
                    updateMasterShard(toCreateDcGroup.dcCluster.groupName);
                    updateAllMasterShards();
                }
            }

        });

        $('#createDcGroupModal').modal('hide');
    }

    function isAlreadyExistDcGroup(dcName) {
        var exist = false;
        for (var index = 0; index < $scope.dcClusterModels.length; index++) {
            if ($scope.dcClusterModels[index].dc.dc_name == dcName) {
               exist = true;
               break;
            }
        }
        return exist;
    }

    function toCreateDcGroupNameChange(index) {
        $scope.toCreateDcGroups[index].dcCluster.groupName = $scope.toCreateDcGroups[index].dc.dc_name;
    }

    function preDeleteDcGroup(index) {
        $scope.toDeleteDcGroupIndex = index;
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            deleteDcGroup();
        } else {
            $('#deleteDcGroupConfirm').modal('show');
        }
    }

    function deleteDcGroup() {
        if($scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dc.dc_name == $scope.activeDcName) {
            toastr.error('can not delete active dc');
            $('#deleteDcGroupConfirm').modal('hide');
            return ;
        }
        $scope.needCheckDrMasterShards = false;
        if (isMasterGroup($scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dcCluster.groupType)){
            $scope.masterShardNum[$scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dcCluster.groupName] = 0;
            updateMasterShard($scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dcCluster.groupName);
        } else if (isDrMasterGroup($scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dcCluster.groupType)){
            removeDcFromDrMasterDcs($scope.dcClusterModels[$scope.toDeleteDcGroupIndex].dc.dc_name);
            $scope.needCheckDrMasterShards = true;
        }
        $scope.dcClusterModels.splice($scope.toDeleteDcGroupIndex, 1);
        $scope.clusterRelatedDcNames.splice($scope.toDeleteDcGroupIndex, 1);

        updateAllMasterShards();
        updateAllDrMasterShards($scope.needCheckDrMasterShards);
        if ($scope.operateType !== OPERATE_TYPE.CREATE) {
            $('#deleteDcGroupConfirm').modal('hide');
        }
    }

    function removeDcFromDrMasterDcs(dcName) {
        var index = -1;
        for (var i = 0; i < $scope.drMasterDcs.length; i++) {
            if ($scope.drMasterDcs[i] == dcName) {
                index = i;
                break;
            }
        }

        if (index != -1) {
            $scope.drMasterDcs.splice(i, 1);
        }
    }
    function preUpdateDcGroup(dcName) {
        $scope.toUpdateDcGroup=[];
        for(var i in $scope.dcClusterModels) {
            if($scope.dcClusterModels[i].dc.dc_name == dcName) {
                $scope.toUpdateDcGroup = {
                     "dc":  {
                         "dc_name":$scope.dcClusterModels[i].dc.dc_name
                     },
                     "dcCluster" : {
                         "groupName": $scope.dcClusterModels[i].dcCluster.groupName,
                         "groupType": $scope.dcClusterModels[i].dcCluster.groupType
                     },
                     "shardNum": $scope.dcClusterModels[i].shardNum
                 };
                break;
            }
        }

        $('#updateDcGroupModal').modal('show');
    }

    function confirmUpdateDcGroup() {
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            updateDcGroup();
        } else {
            $('#updateDcGroupConfirm').modal('show');
        }
    }

    function updateDcGroup() {
        $scope.needCheckDrMasterShards = false;
        for(var i in $scope.dcClusterModels) {
            if($scope.dcClusterModels[i].dc.dc_name == $scope.toUpdateDcGroup.dc.dc_name) {
                if (isDrMasterGroup($scope.toUpdateDcGroup.dcCluster.groupType)) {
                    if ($scope.toUpdateDcGroup.dcCluster.groupType != $scope.dcClusterModels[i].dcCluster.groupType) { //Master ---> DrMaster
                        $scope.masterShardNum[$scope.dcClusterModels[i].dcCluster.groupName] = 0;
                        updateMasterShard($scope.toUpdateDcGroup.dcCluster.groupName);
                        $scope.drMasterDcs.push($scope.toUpdateDcGroup.dc.dc_name);
                    }
                    $scope.drMasterShardNum = $scope.toUpdateDcGroup.shardNum;
                    updateDrMasterShard();
                } else if (isMasterGroup($scope.toUpdateDcGroup.dcCluster.groupType)){
                    if ($scope.toUpdateDcGroup.dcCluster.groupType != $scope.dcClusterModels[i].dcCluster.groupType) {//DrMaster ---> Master
                        $scope.needCheckDrMasterShards = true;
                        removeDcFromDrMasterDcs($scope.toUpdateDcGroup.dc.dc_name);
                    }
                    $scope.masterShardNum[$scope.toUpdateDcGroup.dcCluster.groupName] = $scope.toUpdateDcGroup.shardNum;
                    updateMasterShard($scope.toUpdateDcGroup.dcCluster.groupName);
                }
                $scope.dcClusterModels[i] = $scope.toUpdateDcGroup;
                updateAllMasterShards();
                updateAllDrMasterShards($scope.needCheckDrMasterShards);
                break;
            }
        }
        if ($scope.operateType !== OPERATE_TYPE.CREATE) {
            $('#updateDcGroupConfirm').modal('hide');
        }
        $('#updateDcGroupModal').modal('hide');
    }

    function removeToCreateDcGroups(index) {
        $scope.toCreateDcGroups.splice(index, 1);
    }

    function addOtherDcGroup() {
        $scope.toCreateDcGroups.push({
                "dc":  {
                    "dc_name":$scope.allDcNames[0]
                },
                "dcCluster" : {
                    "groupName": $scope.allDcNames[0],
                    "groupType": $scope.groupTypes[1]
                },
                "shardNum": 0
            });
    }

    function updateDrMasterShard() {
        if ($scope.drMasterShardNum > $scope.drMasterShards.length) {
            for (var i = $scope.drMasterShards.length; i < $scope.drMasterShardNum; i++) {
                var shardName = $scope.cluster.clusterName + '_' + (i + 1);
                $scope.drMasterShards.push({
                    "shardName" : shardName,
                    "setinelMonitorName" : shardName
                });
            }
        } else if ($scope.drMasterShardNum < $scope.drMasterShards.length) {
            var len = $scope.drMasterShards.length - $scope.drMasterShardNum;
            $scope.drMasterShards.splice($scope.drMasterShardNum, len);
        }

        $scope.dcClusterModels.forEach(function(dcClusterModel) {
            if (isDrMasterGroup(dcClusterModel.dcCluster.groupType)) {
                dcClusterModel.shardNum = $scope.drMasterShardNum;
            }
        });
    }

    function updateMasterShard(groupName) {
        if (typeof($scope.masterShards[groupName]) == 'undefined') {
            $scope.masterShards[groupName] = [];
            for (var i = 0; i < $scope.masterShardNum[groupName] ; i++) {
                var shardName = $scope.cluster.clusterName + '_' + groupName + '_' + (i + 1);
                $scope.masterShards[groupName].push({
                    "shardGroup" : groupName,
                    "groupIndex" : i,
                    "shardName" : shardName,
                    "setinelMonitorName" : shardName
                });
            }
        } else if ($scope.masterShardNum[groupName] > $scope.masterShards[groupName].length) {
            for (var i:number =$scope.masterShards[groupName].length; i < $scope.masterShardNum[groupName]; i++) {
                var shardName = $scope.cluster.clusterName + '_' + groupName + '_' + (i + 1);
                $scope.masterShards[groupName].push({
                    "shardGroup" : groupName,
                    "groupIndex" : i,
                    "shardName" : shardName,
                    "setinelMonitorName" : shardName
                });
            }
        }else if ($scope.masterShardNum[groupName] < $scope.masterShards[groupName].length) {
            var len = $scope.masterShards[groupName].length - $scope.masterShardNum[groupName];
            $scope.masterShards[groupName].splice($scope.masterShardNum[groupName], len);
        }
    }

    function updateAllMasterShards() {
        $scope.allMasterShards = [];
        $scope.dcClusterModels.forEach(function(dcClusterModel) {
            if (isMasterGroup(dcClusterModel.dcCluster.groupType)) {
                $scope.masterShards[dcClusterModel.dcCluster.groupName].forEach(function(masterShard) {
                   $scope.allMasterShards.push(masterShard);
                });
            }
        });
    }

    function updateAllDrMasterShards(needCheckDrMasterShards) {
        if (needCheckDrMasterShards && !isExistDrMaster()) {
            $scope.drMasterShardNum = 0;
            $scope.drMasterShards = [];
        }
    }

    function isExistDrMaster() {
        for(var i in $scope.dcClusterModels) {
            if (isDrMasterGroup($scope.dcClusterModels[i].dcCluster.groupType)) {
                return true;
            }
        }
        return false;
    }

    function preUpdateDrMasterShardModel(index) {
        $scope.toUpdateShardModel = [];
        $scope.toUpdateShardModel = {
            "shardType" : $scope.groupTypes[1],
            "shardIndex": index,
            "shardName" : $scope.drMasterShards[index].shardName,
            "setinelMonitorName" : $scope.drMasterShards[index].setinelMonitorName
        }

        $('#updateShardModal').modal('show');
    }

    function preUpdateMasterShardModel(index) {
        $scope.toUpdateShardModel = [];
        $scope.toUpdateShardModel = {
            "shardType" : $scope.groupTypes[0],
            "shardIndex": index,
            "shardGroup" : $scope.allMasterShards[index].shardGroup,
            "groupIndex" : $scope.allMasterShards[index].groupIndex,
            "shardName" : $scope.allMasterShards[index].shardName,
            "setinelMonitorName" : $scope.allMasterShards[index].setinelMonitorName
        }

        $('#updateShardModal').modal('show');
    }

    function confirmUpdateShardModel(){
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            updateShardModel();
        } else {
            $('#updateShardModelConfirm').modal('show');
        }
    }

    function updateShardModel() {
        if (isMasterGroup($scope.toUpdateShardModel.shardType)) {
            var newMasterShardIndexModel = {
                "shardGroup" : $scope.toUpdateShardModel.shardGroup,
                "groupIndex" : $scope.toUpdateShardModel.groupIndex,
                "shardName" : $scope.toUpdateShardModel.shardName,
                "setinelMonitorName" : $scope.toUpdateShardModel.setinelMonitorName
            }

            $scope.masterShards[$scope.toUpdateShardModel.shardGroup][$scope.toUpdateShardModel.groupIndex] = newMasterShardIndexModel;
            $scope.allMasterShards[$scope.toUpdateShardModel.shardIndex] = newMasterShardIndexModel;

        } else if (isDrMasterGroup($scope.toUpdateShardModel.shardType)) {
            $scope.drMasterShards[$scope.toUpdateShardModel.shardIndex] = {
                "shardName" : $scope.toUpdateShardModel.shardName,
                "setinelMonitorName" : $scope.toUpdateShardModel.setinelMonitorName
            }
        }
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            $('#updateShardModelConfirm').modal('hide');
        }
        $('#updateShardModal').modal('hide');
    }


    function preCreateReplDirection() {
        $scope.toCreateReplDirections=[];
        $scope.toCreateReplDirections.push({
            "clusterName": $scope.cluster.clusterName,
            "srcDcName": $scope.activeDcName,
            "fromDcName": $scope.activeDcName,
            "toDcName": $scope.activeDcName
        });
        $('#createReplDirectionModal').modal('show');
    }

    function createReplDirection() {
        $scope.toCreateReplDirections.forEach(function(toCreateReplDirection){
            $scope.replDirections.push(toCreateReplDirection);
        });

        $('#createReplDirectionModal').modal('hide');
    }

    function preUpdateReplDirection(index) {
        $scope.updateReplDirectionIndex = index;
        $scope.toUpdateReplDirection = $scope.replDirections[index];
        $('#updateReplDirectionModal').modal('show');
    }

    function confirmUpdateReplDirection() {
        $scope.replDirections[$scope.updateReplDirectionIndex] = $scope.toUpdateReplDirection;
        $scope.updateReplDirectionIndex = 0;
        $scope.toUpdateReplDirection = [];
        $('#updateReplDirectionModal').modal('hide');
    }

    function confirmDeleteReplDirection(index) {
        $scope.toDeleteReplDirectionIndex = index;
        if ($scope.operateType == OPERATE_TYPE.CREATE) {
            deleteReplDirection();
        } else {
            $('#deleteReplDirectionConfirm').modal('show');
        }
    }

    function deleteReplDirection() {
        $scope.replDirections.splice($scope.toDeleteReplDirectionIndex, 1);
        if ($scope.operateType !== OPERATE_TYPE.CREATE) {
           $('#deleteReplDirectionConfirm').modal('hide');
        }
    }

    function removeToCreateReplDirections(index) {
        $scope.toCreateReplDirections.splice(index, 1);
    }

    function addOtherReplDirection() {
        $scope.toCreateReplDirections.push({
            "clusterName": $scope.cluster.clusterName,
            "srcDcName": $scope.activeDcName,
            "fromDcName": $scope.activeDcName,
            "toDcName": $scope.activeDcName,
        });
    }

    function changeIsHeteroCluster() {
        $scope.isHeteroCluster = !$scope.isHeteroCluster;
        clearHeteroInfo();
        clearShardInfo();
    }

    function clearHeteroInfo() {
        $scope.drMasterDcs = [];
        $scope.replDirections = [];
        $scope.dcClusterModels = [];
        $scope.allMasterShards = [];
        $scope.drMasterShards = [];
        $scope.masterShards = [];

    }

    function clearShardInfo() {
        $scope.shards = [];
    }

    function changeHeteroToSymmetryCluster() {
        $scope.isHeteroCluster = !$scope.isHeteroCluster;
        $scope.replDirections = [];

        deleteMasterDcs();
    }

    function deleteMasterDcs() {
        for (var index = 0; index < $scope.dcClusterModels.length; index++) {
            if (isMasterGroup($scope.dcClusterModels[index].dcCluster.groupType)) {
                $scope.masterShardNum[$scope.dcClusterModels[index].dcCluster.groupName] = 0;
                updateMasterShard($scope.dcClusterModels[index].dcCluster.groupName);

                $scope.dcClusterModels.splice(index, 1);
                $scope.clusterRelatedDcNames.splice(index, 1);
                index--;

                updateAllMasterShards();
            }
        }
    }

    function changeSymmetryToHeteroCluster() {
        $scope.isHeteroCluster = !$scope.isHeteroCluster;
    }
}
