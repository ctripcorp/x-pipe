angular
	.module('index')
	.controller('ActiveDcMigrationIndexCtl', ActiveDcMigrationIndexCtl);

ActiveDcMigrationIndexCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', '$interval', 'AppUtil',
	'toastr', 'NgTableParams', 'ClusterService', 'DcService', 'MigrationService', 'ClusterType'];

function ActiveDcMigrationIndexCtl($rootScope, $scope, $window, $stateParams, $interval, AppUtil,
								   toastr, NgTableParams, ClusterService, DcService, MigrationService, ClusterType) {

	$scope.sourceDcSelected = sourceDcSelected;
	$scope.targetDcSelected = targetDcSelected;
	$scope.availableTargetDcs = availableTargetDcs;
	$scope.preMigrate = preMigrate;
	$scope.doMigrate = doMigrate;
	$scope.clusterOrgNameSelected = clusterOrgNameSelected;
	$scope.getMasterUnhealthyClusters = getMasterUnhealthyClusters;
	$scope.applyBatchClusterNames = applyBatchClusterNames;
	$scope.clearBatchClusterNames = clearBatchClusterNames;
	$scope.toggleBatchPanel = function () { $scope.batchState.panelOpen = !$scope.batchState.panelOpen; };
    $scope.migrationSysCheckResp = {};
    $scope.enableMigrationButton = false;
    $scope.pinnedClusterNames = [];
    $scope.batchState = {
        input: '',
        names: [],
        hiddenCount: 0,
        notFoundNames: [],
        panelOpen: false
    };

	init();

	var SUCCESS_STATE = 0;

	var WARNING_STATE = 1;

	function init() {
        checkMigrationSystem();
		DcService.loadAllDcs().then(function(data){
			$scope.dcs = data;
            ClusterService.getInvolvedOrgs().then(function (result) {
                $scope.organizations = result;
                $scope.organizations.push({"orgName": "不选择"});
            });
            if (!!$stateParams.clusters && $stateParams.clusters.length > 0) {
                $scope.pinnedClusterNames = $stateParams.clusters.map(function (c) { return c.clusterName; });
				showClusters($stateParams.clusters, $stateParams.fromDc);
			} else if ($stateParams.clusterName != undefined) {
                $scope.pinnedClusterNames = [$stateParams.clusterName];
                ClusterService.load_cluster($stateParams.clusterName).then(function(cluster) {
                    showClusters([cluster], $stateParams.fromDc || cluster.heteroDefaultFromDc);
                });
			} else {
                $scope.pinnedClusterNames = [];
				MigrationService.getDefaultMigrationCluster().then(showCluster);
			}
		});
        intervalRetriveInfo();
	}

	function getMigrationActiveDcId(cluster) {
		if ($scope.sourceDcInfo && $scope.sourceDcInfo.id) {
			return $scope.sourceDcInfo.id;
		}
		var clusterType = ClusterType.lookup(cluster.clusterType);
		if (clusterType && clusterType.useAzGroupType && cluster.migrationActiveDcId) {
			return cluster.migrationActiveDcId;
		}
		return cluster.activedcId;
	}

	function isHeteroCluster(cluster) {
		var clusterType = ClusterType.lookup(cluster.clusterType);
		return clusterType && clusterType.useAzGroupType;
	}

	function focusDcByCluster(cluster) {
		var activeDcId = getMigrationActiveDcId(cluster);
		$scope.sourceDcInfo = $scope.dcs.filter(function (dcInfo) {
			return dcInfo.id === activeDcId;
		})[0];
		if ($scope.sourceDcInfo) {
			$scope.sourceDc = $scope.sourceDcInfo.dcName;
		}
	}

	function showCluster(cluster) {
		var fromDc = cluster.heteroDefaultFromDc || ($scope.dcs[cluster.activedcId] || '');
		showClusters([cluster], fromDc || undefined);
	}

	function showClusters(clusters, fromDc) {
		if (!clusters || clusters.length == 0) return;
		var resolvedFromDc = fromDc || $stateParams.fromDc;
		if (resolvedFromDc) {
			$scope.sourceDcInfo = $scope.dcs.filter(function (dcInfo) {
				return dcInfo.dcName === resolvedFromDc;
			})[0];
			if ($scope.sourceDcInfo) {
				$scope.sourceDc = $scope.sourceDcInfo.dcName;
			}
		}
		const clusterNames = clusters.map(c => c.clusterName);
		var enrichmentFromDc = $scope.sourceDc || resolvedFromDc;
		ClusterService.findClustersByNames(clusterNames, enrichmentFromDc).then(result=>{
			$scope.clusters = filterMigrationClusters(result);
			if (!resolvedFromDc) {
				focusDcByCluster(clusters[0]);
			}
			$scope.tableParams.reload();
		});
	}

	function filterMigrationClusters(clusters) {
		return clusters.filter(function (c) {
			if (!ClusterType.lookup(c.clusterType).supportMigration) {
				return false;
			}
			if (isHeteroCluster(c) && !c.migrationAzGroupClusterId) {
				return false;
			}
			if (!$scope.sourceDcInfo) return false;
			var clusterActiveDcId = isHeteroCluster(c) ? c.migrationActiveDcId : c.activedcId;
			if (!clusterActiveDcId || clusterActiveDcId != $scope.sourceDcInfo.id) {
				return false;
			}
			return true;
		});
	}

    $scope.$on('$destroy',function(){
        $interval.cancel($scope.scheduledWork);
    });

	$scope.scheduledWork;
    function intervalRetriveInfo(){
        $scope.scheduledWork = $interval(checkMigrationSystem, 1500);
    }

    function checkMigrationSystem() {
        MigrationService.checkMigrationSystem().then(function (value) {
            $scope.migrationSysCheckResp = value;
            if(value.state === SUCCESS_STATE) {
                $scope.enableMigrationButton = true;
                $scope.migrationSysCheckResp.success = true;
            } else if (value.state === WARNING_STATE) {
                $scope.enableMigrationButton = true;
                $scope.migrationSysCheckResp.warning = true;
            } else {
                $scope.enableMigrationButton = false;
                $scope.migrationSysCheckResp.error = true;
            }
        });
	}

    $scope.showErrorMessage = function() {
        if($scope.migrationSysCheckResp.message) {
            $('#errorMessage').modal('show');
        }
    };

    $scope.hideErrorMessage = function() {
        $scope.migrationSysCheckResp.message = '';
        $('#errorMessage').modal('hide');
    };

	$scope.clusterOrgName = '';
	function clusterOrgNameSelected() {
        var dcName = $scope.sourceDc;
        if(dcName) {
        	sourceDcSelected();
        }
    }

    $scope.masterUnhealthyClusters = [];
    $scope.masterUnhealthyClusterStateLevels = [
		{"name": "至少一个Master不可用", "level": "LEAST_ONE_DOWN"},
        {"name": "25%以上Master不可用", "level": "QUARTER_DOWN"},
        {"name": "50%以上Master不可用", "level": "HALF_DOWN"},
        {"name": "75%以上Master不可用",  "level": "THREE_QUARTER_DOWN"},
        {"name": "100%Master不可用",  "level": "FULL_DOWN"},
        {"name": "不选",  "level": "NORMAL"}
	];
    $scope.masterUnhealthyClusterState = '';

    function getMasterUnhealthyClusters() {
    	var level = $scope.masterUnhealthyClusterState;
        if(level && level !== "NORMAL") {
            ClusterService.getMasterUnhealthyClusters(level)
                .then(function (targetClusters) {
                    $scope.masterUnhealthyClusters = targetClusters;
                    if($scope.sourceDc) {
                        sourceDcSelected();
                    }
                });
        } else if(level === "NORMAL") {
            $scope.masterUnhealthyClusters = [];
            if($scope.sourceDc) {
                sourceDcSelected();
            }
		}
	}

	function sourceDcSelected() {
		var dcName = $scope.sourceDc;
        var orgName = $scope.clusterOrgName;
        var clusterNameFilter = $scope.masterUnhealthyClusters;
        var level = $scope.masterUnhealthyClusterState;
        $scope.sourceDcInfo = $scope.dcs.filter(function (dcInfo) {
        	return dcInfo.dcName === dcName;
		})[0];

        var hasPinned = $scope.pinnedClusterNames && $scope.pinnedClusterNames.length > 0;
        var hasBatch = $scope.batchState.names && $scope.batchState.names.length > 0;
        var clusterQuery;
        if (hasPinned) {
            clusterQuery = ClusterService.findClustersByNames($scope.pinnedClusterNames, dcName);
        } else if (hasBatch) {
            clusterQuery = ClusterService.findClustersByNames($scope.batchState.names, dcName);
        } else {
            clusterQuery = ClusterService.findClustersByActiveDcName(dcName);
        }

		clusterQuery.then(function (data) {
			var result = data;
			if(orgName && orgName !== "不选择") {
                result = result.filter(function (localCluster) {
                    return localCluster.clusterOrgName === orgName;
                });
            }
            if(level && level !== "NORMAL") {
				result = result.filter(function (localCluster) {
                    return clusterNameFilter.includes(localCluster.clusterName);
                });
			}
            $scope.clusters = filterMigrationClusters(result);
            updateBatchStats();
			$scope.tableParams.reload();
		}, function () {
			$scope.clusters = [];
			if (hasBatch) {
				$scope.batchState.notFoundNames = ($scope.batchState.names || []).slice();
				$scope.batchState.hiddenCount = $scope.batchState.notFoundNames.length;
				toastr.error('部分集群名不存在,请检查输入');
			} else {
				toastr.error('查询集群失败');
			}
			$scope.tableParams.reload();
		});

	}

	function updateBatchStats() {
		if (!$scope.batchState.names || $scope.batchState.names.length === 0) {
			$scope.batchState.hiddenCount = 0;
			$scope.batchState.notFoundNames = [];
			return;
		}
		var visibleNames = {};
		($scope.clusters || []).forEach(function (c) { visibleNames[c.clusterName] = true; });
		var notFound = [];
		for (var i = 0; i < $scope.batchState.names.length; ++i) {
			if (!visibleNames[$scope.batchState.names[i]]) {
				notFound.push($scope.batchState.names[i]);
			}
		}
		$scope.batchState.hiddenCount = notFound.length;
		$scope.batchState.notFoundNames = notFound;
	}

	function applyBatchClusterNames() {
		var raw = $scope.batchState.input || '';
		var parts = raw.split(/[,,\s]+/);
		var names = [];
		var seen = {};
		for (var i = 0; i < parts.length; ++i) {
			var name = (parts[i] || '').trim();
			if (name && !seen[name]) {
				seen[name] = true;
				names.push(name);
			}
		}
		$scope.batchState.names = names;
		if (names.length === 0) {
			if ($scope.sourceDc) sourceDcSelected();
			return;
		}
		if ($scope.sourceDc) {
			sourceDcSelected();
			return;
		}
		ClusterService.findClustersByNames(names, '').then(function (probeResult) {
			var probeClusters = probeResult || [];
			if (probeClusters.length === 0) {
				toastr.error('未找到任何有效集群');
				return;
			}
			var firstCluster = probeClusters[0];
			var guessedFromDc = firstCluster.heteroDefaultFromDc
				|| ($scope.dcs.filter(function (d) { return d.id === firstCluster.activedcId; })[0] || {}).dcName;
			if (!guessedFromDc) {
				toastr.error('无法自动推断源机房,请先选择源机房再应用筛选');
				return;
			}
			$scope.sourceDc = guessedFromDc;
			sourceDcSelected();
		}, function () {
			toastr.error('部分集群名不存在,请检查输入');
			$scope.batchState.notFoundNames = names.slice();
			$scope.batchState.hiddenCount = names.length;
		});
	}

	function clearBatchClusterNames() {
		$scope.batchState.input = '';
		$scope.batchState.names = [];
		$scope.batchState.hiddenCount = 0;
		$scope.batchState.notFoundNames = [];
		if ($scope.sourceDc) {
			sourceDcSelected();
		}
	}

	function targetDcSelected(cluster) {
		if(cluster.targetDc == "-") {
			cluster.selected = false;
		} else {
			cluster.selected = true;
		}
	}

	function availableTargetDcs(cluster) {
		var dcs = [];
		var sourceAzGroupClusterId = isHeteroCluster(cluster) ? Number(cluster.migrationAzGroupClusterId) : null;
		var sourceDcName = $scope.sourceDcInfo.dcName;

		cluster.dcClusterInfo.forEach(function(dcCluster) {
			if (!dcCluster.dcInfo) {
				return;
			}
			if (sourceAzGroupClusterId && Number(dcCluster.azGroupClusterId) !== sourceAzGroupClusterId) {
				return;
			}
			if(dcCluster.dcInfo.dcName !== sourceDcName) {
				dcs.push(dcCluster.dcInfo);
			}
		});

		return dcs;
	}

	function getClusterFromDcId(cluster) {
		if (isHeteroCluster(cluster) && cluster.migrationActiveDcId) {
			return cluster.migrationActiveDcId;
		}
		return cluster.activedcId;
	}

	function validateSelectedFromDcConsistency(selectedClusters) {
		if (!selectedClusters || selectedClusters.length === 0) {
			return true;
		}
		var fromDcSet = {};
		selectedClusters.forEach(function(cluster) {
			var fromDcId = getClusterFromDcId(cluster);
			if (fromDcId !== undefined && fromDcId !== null && fromDcId !== '') {
				fromDcSet[fromDcId] = true;
			}
		});
		var fromDcKeys = Object.keys(fromDcSet);
		if (fromDcKeys.length > 1) {
			toastr.error('所选集群源机房不一致，请按相同源机房筛选后重试');
			return false;
		}
		if ($scope.sourceDcInfo && $scope.sourceDcInfo.id && fromDcKeys.length === 1
				&& fromDcKeys[0] != $scope.sourceDcInfo.id) {
			toastr.error('所选集群源机房与当前源机房不一致，请重新选择源机房后重试');
			return false;
		}
		return true;
	}

	function preMigrate() {
		var selectedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected;
		});
		if (!validateSelectedFromDcConsistency(selectedClusters)) {
			return;
		}
		var targetedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected && (cluster.targetDc !== "-");
		});
		if(! (selectedClusters.length === targetedClusters.length)) {
			$('#createEventWithLostConfirm').modal('show');
		} else {
			doMigrate();
		}

	}

	function doMigrate() {
		var selectedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected && (cluster.targetDc !== "-");
		});
		if (!validateSelectedFromDcConsistency(selectedClusters)) {
			return;
		}

		var migrationClusters = [];
		selectedClusters.forEach(function(cluster) {
			migrationClusters.push({
				clusterId : cluster.id,
				sourceDcId : getMigrationActiveDcId(cluster),
				destinationDcId : getDcId(cluster.targetDc),
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

	function getDcId(destinationDc) {
		var res = -1;
		$scope.dcs.forEach(function(dc) {
			if(dc.dcName === destinationDc) {
				res = dc.id;
			}
		});
		return res;
	}

	$scope.sourceDc = '';

	$scope.clusters = [];

	$scope.sourceDcInfo = {};

	$scope.toggle = function (cluster) {
		cluster.selected = !cluster.selected;
	};

	$scope.isIndeterminate = function() {
		var selectedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected;
		});
		return (selectedClusters.length !== $scope.clusters.length) &&
				(selectedClusters.length > 0);
	};

	$scope.isChecked = function() {
		var selectedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected;
		});
		return (selectedClusters.length === $scope.clusters.length) &&
				(selectedClusters.length !== 0);
	};

	$scope.toggleAll = function() {
		if($scope.isIndeterminate()) {
			$scope.clusters.forEach(function(cluster){
			cluster.selected = true;
			});
		} else {
			$scope.clusters.forEach(function(cluster){
			cluster.selected = !cluster.selected;
			});
		}
	};


	$scope.tableParams = new NgTableParams({
        page : 1,
        count : 10
    }, {
        filterDelay:100,
        getData : function(params) {
        	// TODO [marsqing] paging control
            // params.total(1);
            return $scope.clusters;
        }
    });
}
