angular
    .module('index')
    .controller('ClusterListCtl', ClusterListCtl);

ClusterListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', '$state', 'AppUtil',
    'toastr', 'ClusterService', 'MigrationService', 'DcService', 'LogicalBuService', 'NgTableParams', 'ngTableEventsChannel', 'ClusterType', 'HealthCheckService'];

function ClusterListCtl($rootScope, $scope, $window, $stateParams, $state, AppUtil,
                        toastr, ClusterService, MigrationService, DcService, LogicalBuService, NgTableParams, ngTableEventsChannel, ClusterType, HealthCheckService) {
    const SUCCESS_STATE = 0;
    const UNBOUND_LOGICAL_BU_LABEL = '未绑定';
    $rootScope.currentNav = '1-2';
    $scope.dcs = {};
    $scope.dcsFilterData = [];
    $scope.organizationNames = [];
    $scope.logicalBuNameMap = {0: UNBOUND_LOGICAL_BU_LABEL};
    $scope.logicalBuFilterData = [{id: UNBOUND_LOGICAL_BU_LABEL, title: UNBOUND_LOGICAL_BU_LABEL}];
    $scope.clusterId = $stateParams.clusterId;
    $scope.clusterName = $stateParams.clusterName;
    $scope.containerId = $stateParams.keepercontainer;
    $scope.selectDisplayed= selectDisplayed;
    $scope.selectFiltered = selectFiltered;
    $scope.selectAll = selectAll;
    $scope.unselectAll = unselectAll;
    $scope.getClusterActiveDc = getClusterActiveDc;
    $scope.getClusterFromDc = getClusterFromDc;
    $scope.goMigration = goMigration;
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
    $scope.clusterType = $stateParams.clusterType;
    $scope.clusterTypes = ClusterType.selectData()
    $scope.gotoClusterHickwall = gotoClusterHickwall;
    $scope.showDetails = false;
    $scope.showClusterDetails = showClusterDetails;

    $scope.displayedClusters = [];
    $scope.filteredClusters = [];
    $scope.sourceClusters = [];

    initClusterList();

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
        });

    ngTableEventsChannel.onAfterDataFiltered(function (params, filtered) {
        const index = params.page() - 1;
        const size = params.count();
        const start = index * size;
        const end = Math.min(start + size, filtered.length);
        $scope.filteredClusters = filtered;
        $scope.displayedClusters = filtered.slice(start, end);
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
        if (clusterType && (clusterType.multiActiveDcs || clusterType.isCrossDc)) {
            return "-"
        }
        if (isHeteroCluster(cluster) && cluster.heteroActiveDcSummary) {
            return cluster.heteroActiveDcSummary;
        }

        return $scope.dcs[cluster.activedcId] || "Unbind";
    }

    function isHeteroCluster(cluster) {
        var clusterType = ClusterType.lookup(cluster.clusterType);
        return clusterType && clusterType.useAzGroupType;
    }

    function getClusterFromDc(cluster) {
        if (isHeteroCluster(cluster) && cluster.heteroDefaultFromDc) {
            return cluster.heteroDefaultFromDc;
        }
        return $scope.dcs[cluster.activedcId] || '';
    }

    function goMigration(cluster) {
        const fromDc = getClusterFromDc(cluster);
        if (!fromDc) {
            toastr.error('无法确定集群源机房，请刷新后重试');
            return;
        }
        $state.go('migration_index', {
            clusterName: cluster.clusterName,
            fromDc: fromDc
        });
    }

    function matchesActiveDcFilter(cluster, filterDcId) {
        if (filterDcId === undefined || filterDcId === null || filterDcId === '') {
            return true;
        }
        if (isHeteroCluster(cluster)) {
            if (!cluster.heteroActiveDcIds || cluster.heteroActiveDcIds.length === 0) {
                return false;
            }
            return cluster.heteroActiveDcIds.some(function(id) {
                return id == filterDcId;
            });
        }
        return cluster.activedcId == filterDcId;
    }
    
    function isBiDirectionOrOneWayCluster(type) {
        var clusterType = ClusterType.lookup(type)
        return "bi_direction" == clusterType.value || "one_way" == clusterType.value;
    }
    
    function gotoClusterHickwall(type, clusterName) {
        if(isBiDirectionOrOneWayCluster(type)) {
            ClusterService.getClusterHickwallAddr(clusterName, type).then(function(result) {
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
        if (selected.length === 0) {
            toastr.warning('请先勾选要迁移的集群');
            return;
        }
        let fromDcSet = {};
        selected.forEach(function(cluster) {
            fromDcSet[getClusterFromDc(cluster)] = true;
        });
        let fromDcKeys = Object.keys(fromDcSet).filter(function(key) {
            return key !== '';
        });
        if (fromDcKeys.length !== 1) {
            if (fromDcKeys.length > 1) {
                toastr.error('所选集群源机房不一致，请按相同源机房筛选后重试');
            } else {
                toastr.error('无法确定所选集群源机房，请刷新后重试');
            }
            return;
        }
        $state.go('migration_index', { clusters: selected, fromDc: fromDcKeys[0] });
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
            return showUnhealthyClusters();
        } else if (type === "showErrorMigrating") {
            return showErrorMigratingClusters();
        } else if (type === 'showMigrating') {
            return showMigratingClusters();
        } else {
            if ($scope.dcName){
                if ($scope.type === "activeDC"){
                    return showClustersByActiveDc($scope.dcName);
                }else if ($scope.type === "bindDC"){
                    return showClustersBindDc($scope.dcName);
                }
            }
            else if ($scope.containerId) {
                return showClustersByContainer($scope.containerId)
            }
            else {
                return showAllClusters();
            }
        }
    }

    function showUnhealthyClusters() {
        return ClusterService.getUnhealthyClusters().then(loadTable).then(() => { $scope.showUnhealthy = true; });
    }

    function showErrorMigratingClusters() {
        return ClusterService.getErrorMigratingClusters().then(loadTable).then(() => { $scope.showErrorMigrating = true; });
    }

    function showMigratingClusters() {
        return ClusterService.getMigratingClusters().then(loadTable).then(() => { $scope.showMigrating = true; });
    }

    function showAllClusters() {
        return ClusterService.findAllClusters().then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersBindDc(dcName) {
        return ClusterService.findClustersByDcNameBind(dcName).then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersBindDcAndType(dcName, clusterType) {
        return ClusterService.findClustersByDcNameBindAndType(dcName, clusterType).then(loadTable).then(() => {
            $scope.showAll = true;
        });
    }

    function showClustersByActiveDc(dcName) {
        return ClusterService.findClustersByDcName(dcName).then(loadTable).then(() => { $scope.showAll = true; });
    }

    function showClustersByActiveDcAndType(dcName, clusterType) {
        return ClusterService.findClustersByDcNameAndType(dcName, clusterType).then(loadTable).then(() => {
            $scope.showAll = true;
        });
    }

    function showClustersByContainer(containerId) {
        return ClusterService.findAllByKeeperContainer(containerId).then(loadTable).then(() => { $scope.showAll = true; });
    }

    function initLogicalBuMap(result) {
        (result || []).forEach(function (bu) {
            $scope.logicalBuNameMap[bu.id] = bu.name;
            $scope.logicalBuFilterData.push({
                id: bu.name,
                title: bu.name
            });
        });
    }

    function initClusterList() {
        LogicalBuService.findAll()
            .then(function (result) {
                initLogicalBuMap(result);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '加载逻辑 BU 失败');
            })
            .then(function () {
                if ($scope.clusterName) {
                    return ClusterService.load_cluster($scope.clusterName)
                        .then(function (data) {
                            loadTable([data]);
                            $scope.showAll = true;
                        });
                }
                if ($scope.dcName) {
                    if ($scope.type === "activeDC") {
                        return showClustersByActiveDcAndType($scope.dcName, $scope.clusterType);
                    }
                    if ($scope.type === "bindDC") {
                        return showClustersBindDcAndType($scope.dcName, $scope.clusterType);
                    }
                }
                if ($scope.containerId) {
                    return showClustersByContainer($scope.containerId);
                }
                return showClusters("showAll");
            });
    }

    function enrichClusters(data) {
        return data.map(function (cluster) {
            cluster.logicalBuName = $scope.logicalBuNameMap[cluster.logicalBuId || 0] || UNBOUND_LOGICAL_BU_LABEL;
            return cluster;
        });
    }

    function loadTable(data) {
        $scope.sourceClusters = enrichClusters(data);
        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            filterDelay:100,
            dataset: $scope.sourceClusters,
            filterOptions: {
                filterFn: function(rows, filter) {
                    return rows.filter(function(row) {
                        return Object.keys(filter).every(function(key) {
                            var filterValue = filter[key];
                            if (filterValue === undefined || filterValue === null || filterValue === '') {
                                return true;
                            }
                            if (key === 'activedcId') {
                                return matchesActiveDcFilter(row, filterValue);
                            }
                            if (key === 'clusterType') {
                                var clusterType = ClusterType.lookup(row[key]);
                                return clusterType && clusterType.value === filterValue;
                            }
                            var rowValue = row[key];
                            if (rowValue === undefined || rowValue === null) {
                                return false;
                            }
                            if (key === 'clusterName' || key === 'tag') {
                                return ('' + rowValue).toLowerCase().indexOf(('' + filterValue).toLowerCase()) !== -1;
                            }
                            return rowValue == filterValue;
                        });
                    });
                }
            }
        });
    }

    function showClusterDetails() {
        $scope.showDetails = !$scope.showDetails;
    }
}
