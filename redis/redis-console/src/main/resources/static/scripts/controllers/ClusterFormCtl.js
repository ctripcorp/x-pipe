index_module.controller('ClusterFromCtl', ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
    function ($rootScope, $scope, $stateParams, $window, toastr, AppUtil, ClusterService) {

        $rootScope.currentNav = '1-3';

        var clusterName = $stateParams.clusterName;
        
        $scope.operateType = clusterName ? 'update' : 'create';
        
        if ($scope.operateType == 'update') {
            ClusterService.load_cluster(clusterName)
                .then(function (result) {
                    $scope.cluster = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                })
        } else {
            $scope.cluster = {};
        }

        $scope.doCluster = doCluster;

        function doCluster() {
            
            if ($scope.operateType == 'create'){
                ClusterService.createCluster($scope.cluster)
                    .then(function (result) {
                        toastr.success("创建成功");
                        console.log(result);
                    }, function (result) {
                        toastr.error(AppUtil.errorMsg(result), "创建失败");
                    });    
            }   else{
                ClusterService.updateCluster($scope.cluster.clusterName, $scope.cluster)
                    .then(function (result) {
                        toastr.success("更新成功");
                        console.log(result);
                    }, function (result) {
                        toastr.error(AppUtil.errorMsg(result), "更新失败");
                    });
            }
            
        }

    }]);
