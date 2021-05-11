angular
    .module('index')
    .controller('ClusterListCtl', ClusterListCtl);

ClusterListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'ClusterService', 'MigrationService', 'DcService', 'NgTableParams', 'ClusterType'];

function ClusterListCtl($rootScope, $scope, $window, $stateParams, AppUtil,
                        toastr, ClusterService, MigrationService, DcService, NgTableParams, ClusterType) {

    $rootScope.currentNav = '1-2';
    $scope.select = {};
    $scope.dcs = {};
    $scope.clusterId = $stateParams.clusterId;
    $scope.clusterName = $stateParams.clusterName;
    $scope.containerId = $stateParams.keepercontainer;
    $scope.getClusterActiveDc = getClusterActiveDc;
    $scope.getTypeName = getTypeName;
    $scope.preDeleteCluster = preDeleteCluster;
    $scope.deleteCluster = deleteCluster;
    $scope.preResetClusterStatus = preResetClusterStatus;
    $scope.resetClusterStatus = resetClusterStatus;
    $scope.preResetSelectedClusterStatus = preResetSelectedClusterStatus;
    $scope.resetSelectedClusterStatus = resetSelectedClusterStatus;
    $scope.preContinueSelectedCluster = preContinueSelectedCluster;
    $scope.continueSelectedCluster = continueSelectedCluster;
    $scope.preForceSelectedCluster = preForceSelectedCluster;
    $scope.forceSelectedCluster = forceSelectedCluster;
    $scope.getSelectedClusters = getSelectedClusters;
    $scope.showClusters = showClusters;
    $scope.showAll = false;
    $scope.showUnhealthy = false;
    $scope.showErrorMigrating = false;
    $scope.showMigrating = false;
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
        showClusters("showAll");
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

    function preResetClusterStatus(clusterName, clusterId) {
        $scope.clusterId = clusterId;
        $scope.clusterName = clusterName;
        $('#resetClusterStatusConfirm').modal('show');
    }

    function resetClusterStatus() {
        ClusterService.resetClusterStatus($scope.clusterId)
            .then(function (result) {
                $('#resetClusterStatusConfirm').modal('hide');
                toastr.success('重置成功');
                setTimeout(function () {
                    // TODO [marsqing] reload ng-table instead of reload window
                    $window.location.reload();
                },1000);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '重置失败');
            })
    }

    function preResetSelectedClusterStatus(clusterName, clusterId) {
        $('#resetSelectedClusterStatusConfirm').modal('show');
    }

    function resetSelectedClusterStatus() {
        let selected = $scope.getSelectedClusters().map(c => c.id);
        ClusterService.resetClusterStatus.apply(ClusterService, selected)
            .then(function (result) {
                $('#resetClusterStatusConfirm').modal('hide');
                toastr.success('重置成功');
                setTimeout(function () {
                    // TODO [marsqing] reload ng-table instead of reload window
                    $window.location.reload();
                },1000);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '重置失败');
            })
    }

    function preContinueSelectedCluster() {
        $('#continueSelectedClusterConfirm').modal('show');
    }

    function continueSelectedCluster() {
        let selected = $scope.getSelectedClusters();
        selected.forEach(cluster => {
            MigrationService.continueMigrationCluster(cluster.migrationEventId, cluster.id);
        });
    }

    function preForceSelectedCluster() {
        $('#forceSelectedClusterConfirm').modal('show');
    }

    function forceSelectedCluster() {
        let selected = $scope.getSelectedClusters();
        selected.forEach(cluster => {
            MigrationService.forceProcessMigrationCluster(cluster.migrationEventId, cluster.id);
        });
    }

    function getSelectedClusters() {
        let selected;
        if ($scope.select.all) {
            selected = $scope.sourceClusters.filterOut(c => c.isChecked === false);
        } else {
            selected = $scope.sourceClusters.filter(c => c.isChecked);
        }
        return selected;
    }

    function migrateSelectedClusters() {
        let selected = $scope.getSelectedClusters();
        let migrationClusters = [];
        selected.forEach(function(cluster) {
            migrationClusters.push({
                clusterId : cluster.id,
                sourceDcId : cluster.activedcId,
                destinationDcId : cluster.back,
                cluster,
            });
        });
        MigrationService.createEvent(migrationClusters)
            .then(function(result) {
                $('#createEventWithLostConfirm').modal('hide');
                toastr.success('创建成功');
                $window.location.href = '/#/migration_event_details/' + result.value;
            }, function(result) {
                toastr.error(AppUtil.errorMsg(result), '创建失败');
            });
    }

    function clearData() {
        loadTable([]);
        $scope.showAll = false;
        $scope.showUnhealthy = false;
        $scope.showErrorMigrating = false;
        $scope.showMigrating = false;
    }

    function showClusters(type) {
        clearData();
        if (type === "showUnhealthy") {
            showUnhealthyClusters();
        } else if (type === "showErrorMigrating") {
            showErrorMigratingClusters();
        } else if (type === 'showMigrating') {
            showMigratingClusters();
        } else {
            if ($scope.dcName){
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
    }

    function showUnhealthyClusters() {
        ClusterService.getUnhealthyClusters().then(loadTable).then(() => { $scope.showUnhealthy = true; });
    }

    function showErrorMigratingClusters() {
        ClusterService.getErrorMigratingClusters().then(loadTable).then(() => { $scope.showErrorMigrating = true; });
    }

    function showMigratingClusters() {
        ClusterService.getMigratingClusters().then(loadTable).then(() => { $scope.showMigrating = true; });
    }

    function showAllClusters() {
        ClusterService.findAllClusters().then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersBindDc(dcName) {
        ClusterService.findClustersByDcNameBind(dcName).then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersByActiveDc(dcName) {
        ClusterService.findClustersByDcName(dcName).then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersByContainer(containerId) {
        ClusterService.findAllByKeeperContainer(containerId).then(loadTable).then(() => { $scope.showAll = true; });
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
}
