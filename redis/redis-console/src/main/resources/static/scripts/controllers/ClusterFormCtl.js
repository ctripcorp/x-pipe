index_module.controller('ClusterFromCtl',
                        ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
                         'DcService',
                         function ($rootScope, $scope, $stateParams, $window, toastr, AppUtil, ClusterService,
                                   DcService) {

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

                             $scope.doCluster = doCluster;
                             $scope.getDcName = getDcName;
                             $scope.preDeleteCluster = preDeleteCluster;
                             $scope.deleteCluster = deleteCluster;
                             $scope.slaveExists = slaveExists;
                             $scope.toggle = toggle;

                             init();

                             function init() {

                                DcService.loadAllDcs()
                                    .then(function (result) {
                                        $scope.allDcs = result;
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
                                 }
                             }

                             function doCluster() {

                                 if ($scope.operateType == OPERATE_TYPE.CREATE) {
                                     ClusterService.createCluster($scope.cluster, $scope.selectedDcs)
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
                     		    }
                     		    else {
                     		    	$scope.selectedDcs.push(dc);
                     		    }
                             }
                         }]);