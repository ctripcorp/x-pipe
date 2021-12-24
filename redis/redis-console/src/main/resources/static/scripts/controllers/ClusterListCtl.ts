angular
    .module('index')
    .controller('ClusterListCtl', ClusterListCtl);

ClusterListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', '$state', 'AppUtil',
    'toastr', 'ClusterService', 'MigrationService', 'DcService', 'NgTableParams', 'ngTableEventsChannel', 'ClusterType', 'HealthCheckService'];

function ClusterListCtl($rootScope, $scope, $window, $stateParams, $state, AppUtil,
                        toastr, ClusterService, MigrationService, DcService, NgTableParams, ngTableEventsChannel, ClusterType, HealthCheckService) {
    const SUCCESS_STATE = 0;
    $rootScope.currentNav = '1-2';
    $scope.dcs = {};
    $scope.dcsFilterData = [];
    $scope.organizationNames = [];
    $scope.clusterId = $stateParams.clusterId;
    $scope.clusterName = $stateParams.clusterName;
    $scope.containerId = $stateParams.keepercontainer;
    $scope.selectDisplayed= selectDisplayed;
    $scope.selectFiltered = selectFiltered;
    $scope.selectAll = selectAll;
    $scope.unselectAll = unselectAll;
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
    $scope.migrateSelectedClusters = migrateSelectedClusters;
    $scope.showClusters = showClusters;
    $scope.showAll = false;
    $scope.showUnhealthy = false;
    $scope.showErrorMigrating = false;
    $scope.showMigrating = false;
    $scope.dcName = $stateParams.dcName;
    $scope.type = $stateParams.type;
    $scope.clusterTypes = ClusterType.selectData()
    $scope.gotoClusterHickwall = gotoClusterHickwall;

    $scope.displayedClusters = [];
    $scope.filteredClusters = [];
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
                $scope.dcsFilterData.push({
                    "id": dc.id,
                    "title": dc.dcName
                });
    		}
    	});

    ClusterService.getOrganizations()
        .then(function (result) {
            for(let i = 0 ; i < result.length; ++i) {
                let org = result[i];
                $scope.organizationNames.push({
                    "id": org.orgName,
                    "title": org.orgName
                });
            }
            console.log($scope.organizationNames);
        });

    ngTableEventsChannel.onAfterDataFiltered(function (params, filtered) {
        const index = params.page() - 1;
        const size = params.count();
        const start = index * size;
        const end = Math.min(start + size, filtered.length);
        $scope.filteredClusters = filtered;
        $scope.displayedClusters = filtered.slice(start, end);
        console.log(index, size, start, end, $scope.filteredClusters, $scope.displayedClusters);
    });

    function selectDisplayed() {
        $scope.displayedClusters.forEach(c => c.isChecked = true);
    }

    function selectFiltered() {
        $scope.filteredClusters.forEach(c => c.isChecked = true);
    }

    function selectAll() {
        $scope.sourceClusters.forEach(c => c.isChecked = true);
    }

    function unselectAll() {
        $scope.sourceClusters.forEach(c => c.isChecked = false);
    }

    function getClusterActiveDc(cluster) {
        var clusterType = ClusterType.lookup(cluster.clusterType)
        if (clusterType && clusterType.multiActiveDcs) {
            return "-"
        }

        return $scope.dcs[cluster.activedcId] || "Unbind";
    }
    
    function isBiDirectionCluster(type) {
        var clusterType = ClusterType.lookup(type)
        return "bi_direction" == clusterType.value
    }
    
    function gotoClusterHickwall(type, clusterName) {
        if(isBiDirectionCluster(type)) {
            ClusterService.getClusterHickwallAddr(clusterName).then(function(result) {
                if(result != null && result.state === SUCCESS_STATE) {
                    $window.open(result.message, '_blank');
                }
            });
        }
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
        return $scope.sourceClusters.filter(c => c.isChecked);
    }

    function migrateSelectedClusters() {
        let selected = $scope.getSelectedClusters();
        $state.go('migration_index', { clusters: selected });
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
