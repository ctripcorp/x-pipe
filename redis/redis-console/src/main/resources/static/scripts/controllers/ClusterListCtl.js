index_module.controller('ClusterListCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'ClusterService','DcService', 'NgTableParams', 'ClusterType',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, ClusterService, DcService, NgTableParams, ClusterType) {

        $rootScope.currentNav = '1-2';
        $scope.dcs = {};
        $scope.clusterName = $stateParams.clusterName;
        $scope.containerId = $stateParams.keepercontainer;
        $scope.getClusterActiveDc = getClusterActiveDc;
        $scope.getTypeName = getTypeName;
        $scope.preDeleteCluster = preDeleteCluster;
        $scope.deleteCluster = deleteCluster;
        $scope.showUnhealthyClusterOnly = false;
        $scope.dcName = $stateParams.dcName;
        $scope.type = $stateParams.type;
        $scope.clusterTypes = ClusterType.selectData()

        $scope.sourceClusters = [];
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


        function getClusterActiveDc(cluster) {
            var clusterType = ClusterType.lookup(cluster.clusterType)
            if (clusterType && clusterType.multiActiveDcs) {
                return "-"
            }

            return $scope.dcs[cluster.activedcId] || "Unbind";
        }

        function getTypeName(type) {
            var clusterType = ClusterType.lookup(type)
            if (clusterType) return clusterType.name
            else return '未知类型'
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
            $scope.sourceClusters = data;
            $scope.tableParams = new NgTableParams({
                page : 1,
                count : 10
            }, {
                filterDelay:100,
                dataset: $scope.sourceClusters,
            });
        }


    }]);
