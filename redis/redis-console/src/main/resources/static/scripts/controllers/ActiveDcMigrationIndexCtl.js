index_module.controller('ActiveDcMigrationIndexCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams', 'ClusterService', 'DcService',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, ClusterService, DcService, $filters) {
		
		$scope.sourceDcSelected = sourceDcSelected;
		$scope.targetDcSelected = targetDcSelected;
		$scope.availableTargetDcs = availableTargetDcs;
		$scope.preMigrate = preMigrate;
		
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
			console.log(selectedClusters);
		}
		
		$scope.sourceDc = '';
		
		$scope.clusters = [];
		
		$scope.toggle = function (cluster) {
			cluster.selected = !cluster.selected;
		};
		
		$scope.isIndeterminate = function() {
			// TODO [marsqing]
			return false;
		};

		$scope.isChecked = function() {
			// TODO [marsqing]
			return false;
		};

		$scope.toggleAll = function() {
			// TODO [marsqing]
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
