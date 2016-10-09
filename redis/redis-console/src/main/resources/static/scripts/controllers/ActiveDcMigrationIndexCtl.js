index_module.controller('ActiveDcMigrationIndexCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, $filters) {
		$scope.dcs=['jq','oy','fq'];
		$scope.sourceDc = '';
		
		$scope.selected = [];
		$scope.clusters = [{
			clusterName : 'cluster1',
			dcs : ['jq','oy','fq']
			},
			{
			clusterName : 'cluster2',
			dcs : ['jq','oy']
		}];
		
		$scope.toggle = function (item, list) {
		    var idx = list.indexOf(item);
		    if (idx > -1) {
		      list.splice(idx, 1);
		    }
		    else {
		      list.push(item);
		    }
		};
		$scope.exists = function (item, list) {
		    return list.indexOf(item) > -1;
		};
		$scope.isIndeterminate = function() {
		    return ($scope.selected.length !== 0 &&
		        $scope.selected.length !== $scope.clusters.length);
		};

		$scope.isChecked = function() {
		    return $scope.selected.length === $scope.clusters.length;
		};

		$scope.toggleAll = function() {
		    if ($scope.selected.length === $scope.clusters.length) {
		      $scope.selected = [];
		    } else if ($scope.selected.length === 0 || $scope.selected.length > 0) {
		      $scope.selected = $scope.clusters.slice(0);
		    }
		};
		
		
		$scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            filterDelay:100,
            getData : function(params) {
                params.total(2);
                return $scope.clusters;
            }
        });
    }]);
