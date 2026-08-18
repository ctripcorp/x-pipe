angular
    .module('index')
    .controller('ClusterListCtl', ClusterListCtl);

ClusterListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', '$state', 'AppUtil',
    'toastr', 'ClusterService', 'MigrationService', 'DcService', 'ZoneService', 'LogicalBuService', 'NgTableParams', 'ngTableEventsChannel', 'ClusterType', 'HealthCheckService'];

function ClusterListCtl($rootScope, $scope, $window, $stateParams, $state, AppUtil,
                        toastr, ClusterService, MigrationService, DcService, ZoneService, LogicalBuService, NgTableParams, ngTableEventsChannel, ClusterType, HealthCheckService) {
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

    $scope.zonesById = {};
    $scope.dcZoneName = {};
    $scope.regionDcIds = {};
    $scope.regionFilterData = [];
    $scope.azGroupTypeOptions = (function () {
        var all = ClusterType.selectData().filter(function (opt) { return opt.id !== 'hetero'; });
        var deprecated = {bi_direction: 1, cross_dc: 1};
        var normal = [];
        var tail = [];
        for (var i = 0; i < all.length; ++i) {
            if (deprecated[all[i].id]) tail.push(all[i]);
            else normal.push(all[i]);
        }
        return normal.concat(tail);
    })();
    $scope.azGroupConditions = [];
    $scope.addAzGroupCondition = addAzGroupCondition;
    $scope.removeAzGroupCondition = removeAzGroupCondition;
    $scope.clearAzGroupConditions = clearAzGroupConditions;
    $scope.applyAzGroupConditions = applyAzGroupConditions;
    $scope.dcOptionsForRegion = dcOptionsForRegion;
    $scope.onConditionRegionChange = onConditionRegionChange;
    $scope.toggleConditionDc = toggleConditionDc;
    $scope.isConditionDcChecked = isConditionDcChecked;

    $scope.displayedClusters = [];
    $scope.filteredClusters = [];
    $scope.sourceClusters = [];

    initClusterList();

    var dcs = [];
    var dcById = {};
    DcService.loadAllDcs()
    	.then(function(data) {
    		for(var i = 0 ; i < data.length; ++i) {
    			var dc = data[i];
    			$scope.dcs[dc.id] = dc.dcName;
                $scope.dcsFilterData.push({
                    "id": dc.id,
                    "title": dc.dcName
                });
                dcs.push(dc);
                dcById[dc.id] = dc;
    		}
            rebuildRegionData();
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

    ZoneService.findAllZones()
        .then(function (zones) {
            zones = zones || [];
            for (var i = 0; i < zones.length; ++i) {
                $scope.zonesById[zones[i].id] = zones[i].zoneName;
            }
            rebuildRegionData();
        });

    function rebuildRegionData() {
        if (!dcs.length || Object.keys($scope.zonesById).length === 0) return;
        $scope.dcZoneName = {};
        $scope.regionDcIds = {};
        for (var i = 0; i < dcs.length; ++i) {
            var dc = dcs[i];
            var zoneName = $scope.zonesById[dc.zoneId];
            if (!zoneName) continue;
            $scope.dcZoneName[dc.id] = zoneName;
            if (!$scope.regionDcIds[zoneName]) $scope.regionDcIds[zoneName] = [];
            $scope.regionDcIds[zoneName].push(dc.id);
        }
        var regionKeys = Object.keys($scope.regionDcIds).sort();
        $scope.regionFilterData = [{id: '', title: ''}];
        for (var k = 0; k < regionKeys.length; ++k) {
            $scope.regionFilterData.push({id: regionKeys[k], title: regionKeys[k]});
        }
    }

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

    function getClusterFromDcCandidates(cluster) {
        if (isHeteroCluster(cluster)) {
            var ids = cluster.heteroActiveDcIds || [];
            var summary = cluster.heteroActiveDcSummary || '';
            var parts = summary.split(' / ');
            var candidates = [];
            for (var i = 0; i < ids.length; ++i) {
                var seg = parts[i] || '';
                var type = seg.split(':')[0];
                var ct = type ? ClusterType.lookup(type) : null;
                if (ct && ct.supportMigration) {
                    var dcName = $scope.dcs[ids[i]];
                    if (dcName) candidates.push(dcName);
                }
            }
            return candidates;
        }
        var single = $scope.dcs[cluster.activedcId];
        return single ? [single] : [];
    }

    function migrateSelectedClusters() {
        let selected = $scope.getSelectedClusters();
        if (selected.length === 0) {
            toastr.warning('请先勾选要迁移的集群');
            return;
        }
        var candidatesPerCluster = selected.map(getClusterFromDcCandidates);
        if (candidatesPerCluster.some(function (arr) { return arr.length === 0; })) {
            toastr.error('无法确定所选集群源机房，请刷新后重试');
            return;
        }
        var intersection = candidatesPerCluster.reduce(function (acc, cur) {
            return acc.filter(function (dc) { return cur.indexOf(dc) !== -1; });
        }, candidatesPerCluster[0].slice());
        if (intersection.length === 0) {
            toastr.error('所选集群没有共同的源机房，请分批迁移');
            return;
        }
        $state.go('migration_index', { clusters: selected, fromDc: intersection[0] });
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
                        if (!matchesAzGroupConditions(row)) {
                            return false;
                        }
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

    function matchesAzGroupConditions(cluster) {
        var conds = $scope.azGroupConditions || [];
        if (!conds.length) return true;
        for (var i = 0; i < conds.length; ++i) {
            var c = conds[i];
            if (!c) continue;
            var isEmpty = !c.region && !c.clusterType && (!c.dcIds || !c.dcIds.length) && !c.activeDcId;
            if (isEmpty) continue;
            if (!matchesOneAzGroupCondition(cluster, c)) return false;
        }
        return true;
    }

    function matchesOneAzGroupCondition(cluster, cond) {
        var region = cond.region;
        var typeValue = cond.clusterType;
        var wantDcIds = cond.dcIds || [];
        var wantActiveDcId = cond.activeDcId;
        var hasRegion = !!region;
        var hasType = !!typeValue;
        var hasDc = wantDcIds.length > 0;
        var hasActive = !!wantActiveDcId;
        if (isHeteroCluster(cluster)) {
            var types = cluster.heteroAzGroupTypes || [];
            var actives = cluster.heteroActiveDcIds || [];
            var dcCsvs = cluster.heteroAzGroupDcIdsCsv || [];
            var n = Math.max(types.length, actives.length, dcCsvs.length);
            for (var i = 0; i < n; ++i) {
                var dcIds = parseCsvIds(dcCsvs[i]);
                if (hasRegion) {
                    var hitRegion = false;
                    for (var d = 0; d < dcIds.length; ++d) {
                        if ($scope.dcZoneName[dcIds[d]] === region) { hitRegion = true; break; }
                    }
                    if (!hitRegion) continue;
                }
                if (hasType && !(types[i] && types[i].toLowerCase() === typeValue)) continue;
                if (hasActive && actives[i] != wantActiveDcId) continue;
                if (hasDc && !anyIdMatch(dcIds, wantDcIds)) continue;
                return true;
            }
            return false;
        }
        if (hasType) {
            var ct = ClusterType.lookup(cluster.clusterType);
            if (!ct || ct.value !== typeValue) return false;
        }
        var boundDcIds = collectClusterBindDcIds(cluster);
        if (hasRegion) {
            var regionHit = false;
            for (var b = 0; b < boundDcIds.length; ++b) {
                if ($scope.dcZoneName[boundDcIds[b]] === region) { regionHit = true; break; }
            }
            if (!regionHit) return false;
        }
        if (hasActive && cluster.activedcId != wantActiveDcId) return false;
        if (hasDc && !anyIdMatch(boundDcIds, wantDcIds)) return false;
        return true;
    }

    function parseCsvIds(csv) {
        if (!csv) return [];
        var parts = csv.split(',');
        var out = [];
        for (var i = 0; i < parts.length; ++i) {
            var s = parts[i].trim();
            if (s) out.push(Number(s));
        }
        return out;
    }

    function anyIdMatch(haystack, needles) {
        for (var i = 0; i < needles.length; ++i) {
            for (var j = 0; j < haystack.length; ++j) {
                if (haystack[j] == needles[i]) return true;
            }
        }
        return false;
    }

    function collectClusterBindDcIds(cluster) {
        var ids = [];
        if (cluster.dcClusterInfo && cluster.dcClusterInfo.length) {
            for (var j = 0; j < cluster.dcClusterInfo.length; ++j) {
                ids.push(cluster.dcClusterInfo[j].dcId);
            }
        }
        if (cluster.activedcId && ids.indexOf(cluster.activedcId) === -1) {
            ids.push(cluster.activedcId);
        }
        return ids;
    }

    function addAzGroupCondition() {
        var cond = {region: '', dcIds: [], activeDcId: '', clusterType: '', _dcOpen: false, _dcOptions: []};
        $scope.azGroupConditions.push(cond);
    }

    function onConditionRegionChange(cond) {
        cond.dcIds = [];
        cond.activeDcId = '';
        cond._dcOptions = computeDcOptionsForRegion(cond.region);
    }

    function computeDcOptionsForRegion(region) {
        if (!region || !$scope.regionDcIds[region]) return [];
        var ids = $scope.regionDcIds[region];
        var opts = [];
        for (var i = 0; i < ids.length; ++i) {
            var name = $scope.dcs[ids[i]];
            if (name) opts.push({id: ids[i], title: name});
        }
        opts.sort(function (a, b) { return a.title < b.title ? -1 : (a.title > b.title ? 1 : 0); });
        return opts;
    }

    function dcOptionsForRegion(region) {
        return computeDcOptionsForRegion(region);
    }

    function toggleConditionDc(cond, dcId) {
        if (!cond.dcIds) cond.dcIds = [];
        var idx = cond.dcIds.indexOf(dcId);
        if (idx === -1) cond.dcIds.push(dcId);
        else cond.dcIds.splice(idx, 1);
    }

    function isConditionDcChecked(cond, dcId) {
        return cond && cond.dcIds && cond.dcIds.indexOf(dcId) !== -1;
    }

    function removeAzGroupCondition(index) {
        $scope.azGroupConditions.splice(index, 1);
        applyAzGroupConditions();
    }

    function clearAzGroupConditions() {
        $scope.azGroupConditions = [];
        applyAzGroupConditions();
    }

    function applyAzGroupConditions() {
        if (!$scope.tableParams) return;
        var filtered = ($scope.sourceClusters || []).filter(matchesAzGroupConditions);
        $scope.tableParams.settings({dataset: filtered});
        $scope.tableParams.page(1);
    }

    function showClusterDetails() {
        $scope.showDetails = !$scope.showDetails;
    }

    (function bindOutsideClickToCloseDcPickers() {
        $(document).on('click.azGroupDcPicker', function () {
            var changed = false;
            for (var i = 0; i < $scope.azGroupConditions.length; ++i) {
                if ($scope.azGroupConditions[i]._dcOpen) {
                    $scope.azGroupConditions[i]._dcOpen = false;
                    changed = true;
                }
            }
            if (changed) $scope.$apply();
        });
        $scope.$on('$destroy', function () {
            $(document).off('click.azGroupDcPicker');
        });
    })();
}
