index_module.controller('ClusterFromCtl',
                        ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
                         'DcService', 'SentinelService',
                         function ($rootScope, $scope, $stateParams, $window, toastr, AppUtil, ClusterService,
                                   DcService, SentinelService) {

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

                             init();

                             function init() {

                                DcService.loadAllDcs()
                                    .then(function (result) {
                                        $scope.allDcs = result;
                                        $scope.allDcs.forEach(function(dc) {
                                        	SentinelService.findSentinelsByDc(dc.dcName)
                                        		.then(function(result) {
                                        			$scope.sentinels[dc.dcName] = result;
                                        		}); 
                                        });
                                        
                                    });
                                ClusterService.getOrganizations()
                                .then(function (result) {
                                     $scope.organizations = result;
                                    $scope.organizationNames = result.map(function (org) {
                                        return org.orgName;
                                    });
                                     console.log($scope.organizationNames);
                                 });

                                 if ($scope.operateType != OPERATE_TYPE.CREATE) {
                                     ClusterService.load_cluster(clusterName)
                                         .then(function (result) {
                                             $scope.cluster = result;
                                         }, function (result) {
                                             toastr.error(AppUtil.errorMsg(result));
                                         })
                                 } else {
                                     $scope.cluster = {};
                                     $scope.clusterRelatedDcs = [];
                                 }
                             }

                             function doCluster() {
                                 if ($scope.operateType == OPERATE_TYPE.CREATE) {
                                	 $scope.shards.forEach(function(shard) {
                                		shard.shardTbl = {};
                                		shard.shardTbl.shardName = shard.shardName;
                                		shard.shardTbl.setinelMonitorName = shard.setinelMonitorName;
                                	 });
                                     ClusterService.createCluster($scope.cluster, $scope.selectedDcs, $scope.shards)
                                         .then(function (result) {
                                             toastr.success("创建成功");
                                             $window.location.href =
                                                 "/#/cluster_form?clusterName=" + result.clusterName + "&type=retrieve";
                                         }, function (result) {
                                             toastr.error(AppUtil.errorMsg(result), "创建失败");
                                         });
                                 } else {
                                     ClusterService.updateCluster($scope.cluster.clusterName, $scope.cluster)
                                         .then(function (result) {
                                             toastr.success("更新成功");
                                             $window.location.href =
                                                 "/#/cluster_form?clusterName=" + result.clusterName + "&type=retrieve";
                                         }, function (result) {
                                             toastr.error(AppUtil.errorMsg(result), "更新失败");
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
                            	 if($scope.cluster) {
                            		 if($scope.currentShard) {
                            		     if($scope.currentShard.shardName.indexOf($scope.cluster.clusterName) >=0 ){
                            			    $scope.currentShard.setinelMonitorName = $scope.currentShard.shardName;
                            			 }else{
                            			    $scope.currentShard.setinelMonitorName = $scope.cluster.clusterName + $scope.currentShard.shardName;
                            			 }
                            		 }
                            	 }
                             }
                             
                         }]);
