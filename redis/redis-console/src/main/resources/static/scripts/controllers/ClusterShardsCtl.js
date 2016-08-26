index_module.controller('ClusterShardCtl',
                        ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
                         'ShardService',
                         function ($rootScope, $scope, $stateParams, $window, toastr, AppUtil, ClusterService,
                                   ShardService) {

                             $scope.clusterName = $stateParams.clusterName;

                             $scope.preCreateShard = preCreateShard;

                             $scope.createShard = createShard;

                             $scope.preDeleteShard = preDeleteShard;

                             $scope.deleteShard = deleteShard;

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
                             }

                             function preCreateShard() {
                                 $scope.shard = {};
                                 $('#createShardModal').modal('show');
                             }

                             function createShard() {
                                 ShardService.createShard($scope.clusterName, $scope.shard).then(function (result) {
                                     toastr.success("create success");
                                     $('#createShardModal').modal('hide');
                                     $window.location.reload();
                                 }, function (result) {
                                     toastr.error(AppUtil.errorMsg(result), "create fail");
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
                            	 		toastr.success("delete success");
                            	 		$window.location.reload();
                            	 	}, function (result) {
                            	 		toastr.error(AppUtil.errorMsg(result), "delete fail");
                            	 	});
                             }

                         }]);
