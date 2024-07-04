angular
    .module('index')
    .controller('ClusterDesignatedRoutesUpdateCtl', ClusterDesignatedRoutesUpdateCtl);

ClusterDesignatedRoutesUpdateCtl.$inject = ['$scope', '$stateParams', '$window', '$location', 'toastr', 'AppUtil', 'ClusterService', 'RouteService', 'ClusterType'];

function ClusterDesignatedRoutesUpdateCtl($scope, $stateParams, $window, $location, toastr, AppUtil, ClusterService, RouteService, ClusterType) {

    $scope.clusterName = $stateParams.clusterName;
    $scope.currentDcName = $stateParams.srcDcName;
    $scope.dcs;
    $scope.designatedRoutes = [];
    $scope.allRoutes = [];
    $scope.toAddDesignatedRoute = [];
    $scope.allDcRouteIdsMap = new Map();
    $scope.dstDcs = [];

    $scope.switchDc = switchDc;

    $scope.preAddClusterDesignatedRoute = preAddClusterDesignatedRoute;
    $scope.addClusterDesignatedRoute = addClusterDesignatedRoute;
    $scope.addOtherDesignatedRoutes = addOtherDesignatedRoutes;
    $scope.removeOtherDesignatedRoutes = removeOtherDesignatedRoutes;

    $scope.deleteDesignatedRoute = deleteDesignatedRoute;

    $scope.preSubmitUpdates = preSubmitUpdates;
    $scope.submitUpdates = submitUpdates;

    $scope.getTypeName = getTypeName;


    if ($scope.clusterName) {
        loadClusterRoutes();
    }

    function loadClusterRoutes() {
        ClusterService.findClusterDCs($scope.clusterName)
            .then(function (result) {
                $scope.dcs = result;
                if($scope.currentDcName == 'true')
                    $scope.currentDcName = $scope.dcs[0].dcName;
                loadDcClusterRoutes($scope.currentDcName, $scope.clusterName);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

    }

    function loadDcClusterRoutes(dcName, clusterName) {
        ClusterService.getClusterDesignatedRoutesBySrcDcNameAndClusterName(dcName, clusterName)
            .then(function (result) {
                $scope.designatedRoutes = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function getTypeName(type) {
        if (null == type || "" == type) return ""
        var clusterType = ClusterType.lookup(type)
        if (clusterType) return clusterType.name
        else return '未知类型'
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
        $scope.dstDcs = [];
        $scope.allDcRouteIdsMap=[];
        $scope.dcs.forEach(function(dc){
            if(dc.dcName != $scope.currentDcName) {
                $scope.dstDcs.push(dc.dcName)

                RouteService.getAllActiveRoutesByTagAndDirection('meta', $scope.currentDcName, dc.dcName)
                .then(function(result){
                    var idList = [];
                    result.forEach(function (route) {
                        $scope.allRoutes[route.id] = route;
                        idList.push(route.id);
                    });
                    $scope.allDcRouteIdsMap[dc.dcName] = idList;

                    if ($scope.toAddDesignatedRoutes.length == 0) {
                        $scope.toAddDesignatedRoutes.push(result.shift());
                    }
                },function(result){
                    toastr.error(AppUtil.errorMsg(result));
                });
            }
        })

        $('#addClusterDesignatedRouteModal').modal('show');
    }

    function addOtherDesignatedRoutes() {
        $scope.toAddDesignatedRoutes.push({});
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

    function deleteDesignatedRoute(id) {
        $scope.toDeleteRouteId = [];
        $scope.toDeleteRouteId = id;

        var index = -1;
        var routes = $scope.designatedRoutes;
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

    function preSubmitUpdates() {
        $('#doSubmitUpdates').modal('show');
    }

    function submitUpdates() {
        ClusterService.updateClusterDesignatedRoutes($scope.currentDcName, $scope.clusterName, $scope.designatedRoutes)
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
