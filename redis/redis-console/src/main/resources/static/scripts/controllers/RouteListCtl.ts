angular
    .module('index')
    .controller('RouteListCtl', RouteListCtl);

RouteListCtl.$inject = ['$scope', 'RouteService', 'NgTableParams', '$stateParams'];

function RouteListCtl($scope, RouteService, NgTableParams, $stateParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['META', 'CONSOLE'];

    $scope.srcDcName = $stateParams.srcDcName;
    $scope.dstDcName = $stateParams.dstDcName;

    $scope.switchTag = switchTag;

    $scope.routeSwitch = routeSwitch;




    showAllRoutes();

    function showAllRoutes() {
//         console.log($scope.currentTag);
//         if($scope.currentTag == true) {
            $scope.currentTag = ['META'];
//         }
        loadAllRouteByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function loadAllRouteByTag(tag, srcDcName, dstDcName) {
//         if(!$tag) tag = ['META'];
        if((srcDcName == 'true') && (dstDcName == 'true')) {
            RouteService.getAllActiveRouteRouteByTag(tag).then(function (data) {
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
        } else {
            RouteService.getAllActiveRouteRouteByTagAndDirection(tag, srcDcName, dstDcName).then(function (data) {
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



    }

    function switchTag(tag) {
        $scope.currentTag = tag;
        loadAllRouteByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function routeSwitch() {
        console.log("11111");
        $('#routeSwitch').modal('show');
    }
}