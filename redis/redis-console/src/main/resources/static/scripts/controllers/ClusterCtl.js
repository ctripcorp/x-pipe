index_module.controller('ClusterCtl', ['$rootScope', '$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil', 'ClusterService', 'ShardService','SweetAlert',
    function ($rootScope, $scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService, ShardService, SweetAlert) {

        $scope.dcs, $scope.dcActiveTab, $scope.shards;

        $scope.switchDc = switchDc;
        $scope.loadCluster = loadCluster;
        $scope.loadShards = loadShards;

        $rootScope.currentNav = '1-1';

        $scope.clusterName = $stateParams.clusterName;

        if ($scope.clusterName) {
            loadCluster();
        }
        
        function switchDc(dc) {
            $scope.dcActiveTab = dc.dcName;
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
                    $scope.dcActiveTab = $scope.dcs[0].dcName;

                    loadShards($scope.clusterName, $scope.dcs[0].dcName);

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadShards(clusterName, dcName) {
            ShardService.findShards(clusterName, dcName)
                .then(function (result) {
                    $scope.shards = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }


    }]);
