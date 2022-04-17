angular
    .module('index')
    .controller('ClusterDesignatedRoutesUpdateCtl', ClusterDesignatedRoutesUpdateCtl);

ClusterDesignatedRoutesUpdateCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window','$interval', '$location',
    'toastr', 'AppUtil', 'ClusterService', 'ShardService', 'HealthCheckService', 'ProxyService', 'RouteService', 'ClusterType', 'NgTableParams'];

function ClusterDesignatedRoutesUpdateCtl($rootScope, $scope, $stateParams, $window, $interval, $location, toastr, AppUtil, ClusterService, ShardService, HealthCheckService, ProxyService, RouteService,  ClusterType, NgTableParams) {

    $scope.clusterName = $stateParams.clusterName;
    $scope.currentDcName = $stateParams.srcDcName;
    $scope.dcs;
    $scope.designatedRoutes=[];
    $scope.allRoutes=[];
    $scope.toAddDesignatedRoute=[];

    $scope.switchDc = switchDc;

    $scope.preAddClusterDesignatedRoute = preAddClusterDesignatedRoute;
    $scope.addClusterDesignatedRoute = addClusterDesignatedRoute;
    $scope.addOtherDesignatedRoutes = addOtherDesignatedRoutes;
    $scope.removeOtherDesignatedRoutes = removeOtherDesignatedRoutes;

    $scope.preDeleteDesignatedRoute = preDeleteDesignatedRoute;
    $scope.deleteDesignatedRoute = deleteDesignatedRoute;

    $scope.submitUpdates = submitUpdates;

    if ($scope.clusterName) {
        loadClusterRoutes();
    }

    function loadClusterRoutes() {
        ClusterService.findClusterDCs($scope.clusterName)
            .then(function (result) {
                if (!result || result.length === 0) {
                    $scope.dcs = [];
                    $scope.currentDcName = [];
                    return;
                }
                $scope.dcs = result;
                if($scope.currentDcName == 'true')
                    $scope.currentDcName = $scope.dcs[0].dcName;
                loadDcClusterRoutes($scope.currentDcName, $scope.clusterName);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

    }

    function loadDcClusterRoutes(dcName, clusterName) {
        ClusterService.getClusterDesignatedRoutesByDcNameAndClusterName(dcName, clusterName)
            .then(function (result) {
                $scope.designatedRoutes = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function switchDc(dc) {
        $scope.currentDcName = dc.dcName;
        $scope.toAddDesignatedRoutes = [];
        $scope.allRouteIds=[];
        $scope.allRoutes=[];
        loadDcClusterRoutes($scope.currentDcName, $scope.clusterName);
    }

    function preAddClusterDesignatedRoute() {
        $scope.toAddDesignatedRoutes = [];

        RouteService.getAllActiveRoutesBySrcDc($scope.currentDcName)
            .then(function (result) {
                $scope.allRouteIds=[];
                result.forEach(function (route) {
                    $scope.allRoutes[route.id] = route;
                    $scope.allRouteIds.push(route.id);
                });
                $scope.toAddDesignatedRoutes.push(result.shift());
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

        $('#addClusterDesignatedRouteModal').modal('show');
    }

    function addOtherDesignatedRoutes() {
        $scope.toAddDesignatedRoutes.push({});
        console.log($scope.toAddDesignatedRoutes);
    }

    function removeOtherDesignatedRoutes(index) {
        $scope.toAddDesignatedRoutes.splice(index, 1);
    }

    function addClusterDesignatedRoute() {
        $scope.toAddDesignatedRoutes.forEach(function (route) {
            $scope.designatedRoutes.push($scope.allRoutes[route.id]);
        });
        $scope.toAddDesignatedRoutes=[];
        $('#addClusterDesignatedRouteModal').modal('hide');
        return ;
    }

    function preDeleteDesignatedRoute(id) {
        $scope.toDeleteRouteId = [];
        $scope.toDeleteRouteId = id;
        $('#doDeleteDesignatedRoute').modal('show');
    }

    function deleteDesignatedRoute() {
        var routes = $scope.designatedRoutes;
        var index = -1;
        for(var cnt_route = 0; cnt_route != routes.length; ++cnt_route) {
            if($scope.toDeleteRouteId == routes[cnt_route].id) {
                index = cnt_route;
            }
        }

        if(index != -1) {
            $scope.designatedRoutes.splice(index, 1);
            return;
        }
    }

    function submitUpdates() {
        ClusterService.updateClusterDesignatedRoutes($scope.clusterName, $scope.currentDcName, $scope.designatedRoutes)
            .then(function(result){
                if(result.message == 'success' ) {
                    toastr.success("更新成功");
                    $('#doSwitchrConfirm').modal('hide');
                    setTimeout(function () {
                        $window.location.href = '/#/cluster_routes?clusterName=' + $scope.clusterName + '&dcName=' + $scope.currentDcName;
                    },1000);
                } else {
                    toastr.error(result.message, "更新失败");
                }
            }, function(result) {
                 toastr.error(AppUtil.errorMsg(result), "更新失败");
            });


    }

}