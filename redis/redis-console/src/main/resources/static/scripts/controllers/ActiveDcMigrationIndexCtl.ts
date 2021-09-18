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
				showClusters($stateParams.clusters);
			} else if ($stateParams.clusterName != undefined) {
                ClusterService.load_cluster($stateParams.clusterName).then(showCluster);
			} else {
				MigrationService.getDefaultMigrationCluster().then(showCluster);
			}
		});
        intervalRetriveInfo();
	}

	function focusDcByCluster(cluster) {
		$scope.sourceDcInfo = $scope.dcs.filter(function (dcInfo) {
			return dcInfo.id === cluster.activedcId;
		})[0];
	}

	function showCluster(cluster) {
		showClusters([cluster]);
	}

	function showClusters(clusters) {
		if (!clusters || clusters.length == 0) return;
		focusDcByCluster(clusters[0]);
		const clusterNames = clusters.map(c => c.clusterName);
		ClusterService.findClustersByNames.apply(ClusterService, clusterNames).then(result=>{
			$scope.clusters = result.filter(c => ClusterType.lookup(c.clusterType).supportMigration && !!c.activedcId && !!$scope.sourceDcInfo && c.activedcId == $scope.sourceDcInfo.id);
			$scope.tableParams.reload();
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
            $scope.clusters = result.filter(c => ClusterType.lookup(c.clusterType).supportMigration);
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

		cluster.dcClusterInfo.forEach(function(dcCluster) {
			if(dcCluster.dcInfo.dcName !== $scope.sourceDcInfo.dcName && dcCluster.dcInfo.zoneId === $scope.sourceDcInfo.zoneId) {
				dcs.push(dcCluster.dcInfo);
			}
		});

		return dcs;
	}

	function preMigrate() {
		var selectedClusters = $scope.clusters.filter(function(cluster){
			return cluster.selected;
		});
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

		var migrationClusters = [];
		selectedClusters.forEach(function(cluster) {
			migrationClusters.push({
				clusterId : cluster.id,
				sourceDcId : cluster.activedcId,
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