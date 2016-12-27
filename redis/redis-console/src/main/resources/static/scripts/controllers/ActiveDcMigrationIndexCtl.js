index_module.controller('ActiveDcMigrationIndexCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams', 'ClusterService', 'DcService', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, ClusterService, DcService, MigrationService, $filters) {
		
		$scope.sourceDcSelected = sourceDcSelected;
		$scope.targetDcSelected = targetDcSelected;
		$scope.availableTargetDcs = availableTargetDcs;
		$scope.preMigrate = preMigrate;
		$scope.doMigrate = doMigrate;
		
		init();
		
		function init() {
			DcService.loadAllDcs().then(function(data){
				$scope.dcs = data;
			});
		}
		
		function sourceDcSelected() {
			var dcName = $scope.sourceDc;
			if(dcName) {
				ClusterService.findClustersByActiveDcName(dcName).then(function(data) {
					$scope.clusters = data;
					$scope.tableParams.reload();
				});
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
			
			cluster.dcClusterInfo.forEach(function(dcCluster) {
				if(dcCluster.dcInfo.dcName != $scope.sourceDc) {
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
				return cluster.selected && (cluster.targetDc != "-");
			});
			if(! (selectedClusters.length == targetedClusters.length)) {
				$('#createEventWithLostConfirm').modal('show');
			} else {
				doMigrate();
			}
			
		}

		function doMigrate() {
			var selectedClusters = $scope.clusters.filter(function(cluster){
				return cluster.selected && (cluster.targetDc != "-");
			});

			var migrationClusters = [];
			selectedClusters.forEach(function(cluster) {
				migrationClusters.push({
					clusterId : cluster.id,
					sourceDcId : cluster.activedcId,
					destinationDcId : getDcId(cluster.targetDc)
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
			var res;
			$scope.dcs.forEach(function(dc) {
				if(dc.dcName == destinationDc) {
					res = dc.id;
				}
			});
			return res;
		}
		
		$scope.sourceDc = '';
		
		$scope.clusters = [];
		
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
			return (selectedClusters.length == $scope.clusters.length) &&
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
    }]);
