angular
    .module('index')
    .controller('RouteSwitchCtl', RouteSwitchCtl);

RouteSwitchCtl.$inject = ['$scope', '$window', 'RouteService', 'toastr', 'AppUtil', 'NgTableParams', '$stateParams'];

function RouteSwitchCtl($scope, $window, RouteService, toastr, AppUtil, NgTableParams, $stateParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['META', 'CONSOLE'];

    $scope.currentTag = $stateParams.tag;
    $scope.srcDcName = $stateParams.srcDcName;
    $scope.dstDcName = $stateParams.dstDcName;

    $scope.switchTag = switchTag;

    $scope.doChangeRoute = doChangeRoute;


    showRoutes();

    function showRoutes() {
        if($scope.currentTag = 'true') $scope.currentTag = 'META';
        loadAllRouteDirectionInfoByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function loadAllRouteDirectionInfoByTag(tag, srcDcName, dstDcName) {
        RouteService.getAllActiveRouteRouteByTagAndDirection(tag, srcDcName, dstDcName)
            .then(function (data) {
                if(Array.isArray(data)) $scope.routes = data;
                console.log($scope.routes);
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

    function doChangeRoute() {
        console.log($scope.routes);
        RouteService.updateRoutes($scope.routes).then(function(result) {
            toastr.success("切换成功");
            $window.location.href = "/#/route_direction/route";
        }, function(result) {
            toastr.error(AppUtil.errorMsg(result), "切换失败");
        });
    }
}