index_module.controller('ClusterListCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'ClusterService','DcService', 'NgTableParams',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, ClusterService, DcService, NgTableParams, $filters) {

        $rootScope.currentNav = '1-2';
        $scope.dcs = {};
        $scope.clusterName = $stateParams.clusterName;
        $scope.containerId = $stateParams.keepercontainer;
        $scope.getDcName = getDcName;
        $scope.preDeleteCluster = preDeleteCluster;
        $scope.deleteCluster = deleteCluster;
        $scope.showUnhealthyClusterOnly = false;
        $scope.dcName = $stateParams.dcName;
        $scope.type = $stateParams.type;
        
        var sourceClusters = [], copedClusters = [];
        if($scope.clusterName) {
        	ClusterService.load_cluster($scope.clusterName)
            .then(function (data) {
                loadTable([data])
            });
        }
        else if ($scope.dcName){
            if ($scope.type === "activeDC"){
                showClustersByActiveDc($scope.dcName);
            }else if ($scope.type === "bindDC"){
                showClustersBindDc($scope.dcName);
            }
        }
        else if ($scope.containerId) {
            showClustersByContainer($scope.containerId)
        }
        else {
            showClusters();
        }
        
        DcService.loadAllDcs()
        	.then(function(data) {
        		for(var i = 0 ; i < data.length; ++i) {
        			var dc = data[i];
        			$scope.dcs[dc.id] = dc.dcName;
        		}
        	});
        
        function getDcName(dcId) {
        	return $scope.dcs[dcId] || "Unbind";
        }
        
        function preDeleteCluster(clusterName) {
        	$scope.clusterName = clusterName;
			$('#deleteClusterConfirm').modal('show');
		}
		function deleteCluster() {
			ClusterService.deleteCluster($scope.clusterName)
				.then(function (result) {
					$('#deleteClusterConfirm').modal('hide');
					toastr.success('删除成功');
					setTimeout(function () {
						// TODO [marsqing] reload ng-table instead of reload window
						$window.location.reload();
					},1000);
		         }, function (result) {
					toastr.error(AppUtil.errorMsg(result), '删除失败');
				})
			}

        $scope.refresh = function() {
            showClusters();
        }

        function showClusters() {
            if ($scope.showUnhealthyClusterOnly === true) {
                showUnhealthyClusters();
            } else if ($scope.dcName){
                if ($scope.type === "activeDC"){
                    showClustersByActiveDc($scope.dcName);
                }else if ($scope.type === "bindDC"){
                    showClustersBindDc($scope.dcName);
                }
            }
            else if ($scope.containerId) {
                showClustersByContainer($scope.containerId)
            }
            else {
                showAllClusters();
            }
        }

        function showUnhealthyClusters() {
            ClusterService.getUnhealthyClusters()
                .then(loadTable);
        }

        function showAllClusters() {
            ClusterService.findAllClusters()
                .then(loadTable);
        }

        function showClustersBindDc(dcName) {
            ClusterService.findClustersByDcNameBind(dcName).then(loadTable);
        }

        function showClustersByActiveDc(dcName) {
            ClusterService.findClustersByDcName(dcName).then(loadTable);
        }

        function showClustersByContainer(containerId) {
            ClusterService.findAllByKeeperContainer(containerId).then(loadTable);
        }

        function loadTable(data) {
            sourceClusters = data;
            copedClusters = _.clone(sourceClusters);
            $scope.tableParams = new NgTableParams({
                page : $rootScope.historyPage,
                count : 10
            }, {
                filterDelay:100,
                getData : function(params) {
                    $rootScope.historyPage = params.page();
                    var filter_text = params.filter().clusterName;
                    if(filter_text) {
                        filter_text = filter_text.toLowerCase();
                        var filtered_data = [];
                        for(var i = 0 ; i < sourceClusters.length ; i++) {
                            var cluster = sourceClusters[i];
                            if(cluster.clusterName.toLowerCase().search(filter_text) !== -1) {
                                filtered_data.push(cluster);
                            }
                        }
                        copedClusters = filtered_data;
                    }else {
                        copedClusters = sourceClusters;
                    }

                    params.total(copedClusters.length);
                    return copedClusters.slice((params.page() - 1) * params.count(), params.page() * params.count());
                }
            });
        }


    }]);
