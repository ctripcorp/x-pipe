angular
    .module('index')
    .controller('RouteSwitchCtl', RouteSwitchCtl);

RouteSwitchCtl.$inject = ['$scope', '$window', 'RouteService', 'toastr', 'AppUtil', 'NgTableParams', '$stateParams'];

function RouteSwitchCtl($scope, $window, RouteService, toastr, AppUtil, NgTableParams, $stateParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['meta', 'console'];

    $scope.currentTag = $stateParams.tag;
    $scope.srcDcName = $stateParams.srcDcName;
    $scope.dstDcName = $stateParams.dstDcName;

    $scope.switchTag = switchTag;
    $scope.preDoSwitchRoute = preDoSwitchRoute;
    $scope.doSwtichRoute = doSwtichRoute;


    showRoutes();

    function showRoutes() {
        if($scope.currentTag = 'true') $scope.currentTag = 'meta';
        loadAllRouteDirectionInfoByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function loadAllRouteDirectionInfoByTag(tag, srcDcName, dstDcName) {
        RouteService.getAllActiveRouteRouteByTagAndDirection(tag, srcDcName, dstDcName)
            .then(function (data) {
                if(Array.isArray(data)) $scope.routes = data;
                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10,
                }, {
                    filterDelay : 100,
                    counts : [10, 25, 50, 100],
                    dataset : $scope.routes
                });
            });
    }

    function switchTag(tag) {
        $scope.currentTag = tag;
        loadAllRouteDirectionInfoByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function preDoSwitchRoute() {
        $('#doSwitchrConfirm').modal('show');
    }

    function doSwtichRoute() {
        RouteService.updateRoutes($scope.routes).then(function(result) {
            $('#doSwitchrConfirm').modal('hide');
            toastr.success("切换成功");
            setTimeout(function () {
                $window.location.href = '/#/route_direction/route';
            },1000);
        }, function(result) {
            toastr.error(AppUtil.errorMsg(result), "切换失败");
        });
    }
}
