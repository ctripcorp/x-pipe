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
                             }

                             function preCreateShard() {
                                 $scope.shard = {};
                                 $('#createShardModal').modal('show');
                             }

                             function createShard() {
                                 ShardService.createShard(shard).then(function (result) {
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
                                 toastr.success("delete success");
                                 // ShardService.delete_shard($scope.clusterName, toDeleteShard)
                                 //     .then(function (result) {
                                 //         toastr.success("delete success");
                                 //         $window.location.reload();
                                 //     }, function (result) {
                                 //         toastr.error(AppUtil.errorMsg(result), "delete fail");
                                 //     })

                             }

                         }]);
