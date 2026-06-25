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
    $scope.migrationSysCheckResp = {};
    $scope.enableMigrationButton = false;

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
				showClusters($stateParams.clusters, $stateParams.fromDc);
			} else if ($stateParams.clusterName != undefined) {
                ClusterService.load_cluster($stateParams.clusterName).then(function(cluster) {
                    showClusters([cluster], $stateParams.fromDc || cluster.heteroDefaultFromDc);
                });
			} else {
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
			var activeDcId = getMigrationActiveDcId(c);
			if (!activeDcId || !$scope.sourceDcInfo || activeDcId != $scope.sourceDcInfo.id) {
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

		ClusterService.findClustersByActiveDcName(dcName).then(function (data) {
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
			$scope.tableParams.reload();
		});

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
