index_module.controller('ClusterFromCtl', ['$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'ClusterService',
    function ($scope, $stateParams, $window, toastr, AppUtil, ClusterService) {

        var clusterName = $stateParams.clusterName;
        var operateType = clusterName ? 'update' : 'create';
        if (operateType == 'update') {
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
            
            if (operateType == 'create'){
                ClusterService.createCluster($scope.cluster)
                    .then(function (result) {
                        toastr.success("创建成功");
                        console.log(result);
                    }, function (result) {

                    });    
            }   else{
                ClusterService.updateCluster($scope.cluster.clusterName, $scope.cluster)
                    .then(function (result) {
                        toastr.success("更新成功");
                        console.log(result);
                    }, function (result) {

                    });
            }
            
        }

    }]);
