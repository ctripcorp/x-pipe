index_module.controller('ClusterShardCtl',
                        ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
                         'ShardService', 'SentinelService',
                         function ($rootScope, $scope, $stateParams, $window, toastr, AppUtil, ClusterService,
                                   ShardService, SentinelService) {

                             $scope.clusterName = $stateParams.clusterName;

                             $scope.preCreateShard = preCreateShard;

                             $scope.createShard = createShard;

                             $scope.preDeleteShard = preDeleteShard;

                             $scope.deleteShard = deleteShard;
                             
                             $scope.shardNameChange = shardNameChange;
                             
                             init();

                             function init() {
                                 ClusterService.load_cluster($scope.clusterName)
                                     .then(function (result) {
                                         $scope.cluster = result;
                                     });
                                 ShardService.findClusterShards($scope.clusterName)
                                 	.then(function (result) {
                                 		$scope.shards = result;
                                 	});
                                 ClusterService.findClusterDCs($scope.clusterName)
                              		.then(function(result) {
                              			$scope.clusterDcs = result;
                              			
                              			$scope.sentinels = {};
                              			$scope.clusterDcs.forEach(function(dc) {
                              				SentinelService.findSentinelsByDc(dc.dcName)
                                    		.then(function(result) {
                                    			$scope.sentinels[dc.dcName] = result;
                                    		}); 
                              			});
                              		});
                             }

                             function preCreateShard() {
                                 $scope.shard = {
                                		 sentinels : {}
                                 };
                                 $('#createShardModal').modal('show');
                             }

                             function createShard() {
                            	 $scope.shard.shardTbl = {
                            			 shardName : $scope.shard.shardName,
                            			 setinelMonitorName : $scope.shard.setinelMonitorName
                            	 };
                                 ShardService.createShard($scope.clusterName, $scope.shard).then(function (result) {
                                     toastr.success("创建成功");
                                     $('#createShardModal').modal('hide');
                                     $window.location.reload();
                                 }, function (result) {
                                     toastr.error(AppUtil.errorMsg(result), "创建失败");
                                 })
                             }

                             var toDeleteShard = {};
                             function preDeleteShard(shard) {
                                 toDeleteShard = shard;
                                 $("#deleteShardConfirm").modal('show');
                             }
                             
                             function deleteShard() {
                            	 ShardService.deleteShard($scope.clusterName, toDeleteShard)
                            	 	.then(function(result) {
                            	 		toastr.success("删除成功");
                            	 		$window.location.reload();
                            	 	}, function (result) {
                            	 		toastr.error(AppUtil.errorMsg(result), "删除失败");
                            	 	});
                             }
                             
                             function shardNameChange() {
                            	 $scope.shard.setinelMonitorName = $scope.clusterName + '-' + $scope.shard.shardName;
                             }

                         }]);
