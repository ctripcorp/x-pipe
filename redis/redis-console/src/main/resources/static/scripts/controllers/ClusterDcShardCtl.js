index_module.controller('ClusterCtl', ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService',
    function ($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService, ShardService) {

        $scope.dcs, $scope.dcActiveTab, $scope.shards;
        $scope.clusterName = $stateParams.clusterName;
        
        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShards = loadShards;
        
        if ($scope.clusterName) {
            loadCluster();
        }
        
        function switchDc(dc) {
            $scope.currentDcName = dc.dcName;
            loadShards($scope.clusterName, dc.dcName);
        }

        function loadCluster() {
            ClusterService.findClusterDCs($scope.clusterName)
                .then(function (result) {
                    if (!result || result.length == 0) {
                        $scope.dcs = [];
                        $scope.shards = [];
                        return;
                    }
                    $scope.dcs = result;
                    
                    // TODO [marsqing] do not re-get dc data when switch dc
                    if($scope.dcs && $scope.dcs.length > 0) {
	                    $scope.dcs.forEach(function(dc){
	                    	if(dc.dcName === $stateParams.currentDcName) {
			                    $scope.currentDcName = dc.dcName;
	                    	}
	                    });
	                    
	                    if(!$scope.currentDcName) {
	                    	$scope.currentDcName = $scope.dcs[0].dcName; 
	                    }
                    }
                    

                    loadShards($scope.clusterName, $scope.currentDcName);

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadShards(clusterName, dcName) {
            ShardService.findClusterDcShards(clusterName, dcName)
                .then(function (result) {
                    $scope.shards = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }

    }]);