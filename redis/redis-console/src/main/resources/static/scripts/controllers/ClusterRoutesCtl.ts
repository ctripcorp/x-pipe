angular
    .module('index')
    .controller('ClusterRoutesCtl', ClusterRoutesCtl);

ClusterRoutesCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window','$interval', '$location',
    'toastr', 'AppUtil', 'ClusterService', 'ShardService', 'HealthCheckService', 'ProxyService', 'RouteService', 'ClusterType', 'NgTableParams'];

function ClusterRoutesCtl($rootScope, $scope, $stateParams, $window, $interval, $location, toastr, AppUtil, ClusterService, ShardService, HealthCheckService, ProxyService, RouteService,  ClusterType, NgTableParams) {

    $scope.dcs;
    $scope.clusterName = $stateParams.clusterName;
    $scope.currentDcName = $stateParams.dcName;
    $scope.routeAvail = false;
    $scope.allClusterDesignateRoutes;
    $scope.allRoutes=[];
    $scope.allRouteIds=[];
    $scope.toAddDesignatedRouteId=0;
    $scope.toDeleteDesignatedRouteId=0;

    $scope.switchDc = switchDc;
    $scope.loadDcClusterRoutes = loadDcClusterRoutes;
    $scope.preDeleteDesinatedRoute = preDeleteDesinatedRoute;
    $scope.deleteDesinatedRoute = deleteDesinatedRoute;
    $scope.preAddClusterDesignatedRoute = preAddClusterDesignatedRoute;
    $scope.addClusterDesignatedRoute = addClusterDesignatedRoute;
    $scope.routeChanged = routeChanged;
    $scope.preUpdateClusterDesignatedRoute = preUpdateClusterDesignatedRoute;
    $scope.updateClusterDesignatedRoute = updateClusterDesignatedRoute;


    $scope.tableParams = new NgTableParams({}, {});

    if ($scope.clusterName) {
        loadClusterRoutes();
    }

    function switchDc(dc) {
        $scope.currentDcName = dc.dcName;
        loadDcClusterRoutes($scope.currentDcName, $scope.clusterName);
    }

    function loadClusterRoutes() {
        ClusterService.findClusterDCs($scope.clusterName)
            .then(function (result) {
                if (!result || result.length === 0) {
                    $scope.dcs = [];
                    $scope.allClusterDesignateRoutes = [];
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
                $scope.routes = result;
                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10,
                }, {
                    filterDelay : 100,
                    counts : [10, 25, 50, 100],
                    dataset : $scope.routes
                });
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function preDeleteDesinatedRoute(id) {
        $scope.toDeleteRouteId = id;
        $('#doDeleteDesinatedRoute').modal('show');
    }

    function deleteDesinatedRoute() {
        ClusterService.deleteClusterDesignatedRoutes($scope.clusterName, $scope.toDeleteRouteId)
            .then(function (result) {
                if(result.message == 'success' ) {
                    toastr.success("删除成功");
                    $('#doDeleteDesinatedRoute').modal('hide');
                    setTimeout(function () {
                        $window.location.href = '/#/cluster_routes?clusterName=' + $scope.clusterName + '&dcName=' + $scope.currentDcName;
                    },1000);
                } else {
                    toastr.error(result.message, "删除失败");
                }

            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), "删除失败");
            });
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
                $scope.toAddDesignatedRoute = result.shift();
                $scope.toAddDesignatedRouteId = $scope.toAddDesignatedRoute.id;
//                 $scope.allRoutes = result;
//                 $scope.toAddDesignatedRoutes = $scope.allRoutes[0];
                console.log('1111');
                console.log($scope.allRoutes);
                console.log($scope.toAddDesignatedRouteId)
                console.log($scope.allRouteIds)
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

        $('#addClusterDesignatedRouteModal').modal('show');
    }

    function routeChanged() {
        console.log("2222");
        console.log($scope.toAddDesignatedRouteId);
        $scope.toAddDesignatedRoute = $scope.allRoutes[$scope.toAddDesignatedRouteId];
    }

    function addClusterDesignatedRoute() {
        console.log("addClusterDesignatedRoute-" + $scope.clusterName + "-"+ $scope.toAddDesignatedRouteId);
        ClusterService.addClusterDesinatedRoutes($scope.clusterName, $scope.toAddDesignatedRouteId)
            .then(function (result) {
                if(result.message == 'success' ) {
                    toastr.success("添加成功");
                    $('#addClusterDesignatedRouteModal').modal('hide');
                    setTimeout(function () {
                        $window.location.href = '/#/cluster_routes?clusterName=' + $scope.clusterName + '&dcName=' + $scope.currentDcName;
                    },1000);
                } else {
                    toastr.error(result.message, "添加失败");
                }
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), "添加失败");
            });
    }

    function preUpdateClusterDesignatedRoute(id) {
        $scope.toDeleteDesignatedRouteId= id;
        $scope.toAddDesignatedRouteId = id;

        RouteService.getAllActiveRoutesBySrcDc($scope.currentDcName)
            .then(function (result) {
                $scope.allRouteIds=[];
                result.forEach(function (route) {
                    $scope.allRoutes[route.id] = route;
                    $scope.allRouteIds.push(route.id);
                });
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

        $('#updateClusterDesignatedRouteModal').modal('show');
    }

    function updateClusterDesignatedRoute() {
        console.log("updateClusterDesignatedRoute-" + $scope.clusterName + "-"+ $scope.toDeleteDesignatedRouteId + "-"+ $scope.toAddDesignatedRouteId);
        ClusterService.updateClusterDesinatedRoutes($scope.clusterName, $scope.toDeleteDesignatedRouteId, $scope.toAddDesignatedRouteId)
            .then(function (result) {
                if(result.message == 'success' ) {
                    toastr.success("修改成功");
                    $('#updateClusterDesignatedRouteModal').modal('hide');
                    setTimeout(function () {
                        $window.location.href = '/#/cluster_routes?clusterName=' + $scope.clusterName + '&dcName=' + $scope.currentDcName;
                    },1000);
                } else {
                    toastr.error(result.message, "修改失败");
                }
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), "修改失败");
            });
    }

}