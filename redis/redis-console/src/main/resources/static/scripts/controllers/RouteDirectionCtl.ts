angular
    .module('index')
    .controller('RouteDirectionCtl', RouteDirectionCtl);

RouteDirectionCtl.$inject = ['$scope', 'RouteService', 'NgTableParams'];

function RouteDirectionCtl($scope, RouteService, NgTableParams) {

    $scope.routeDirectionInfos = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['meta', 'console'];
    $scope.switchTag = switchTag;


    showAllRoutes();

    function showAllRoutes() {
        $scope.currentTag = ['meta'];
        loadAllRouteDirectionInfosByTag($scope.currentTag);
    }

    function loadAllRouteDirectionInfosByTag(tag) {
        RouteService.getAllRouteDirectionInfosByTag(tag)
            .then(function (data) {
                if(Array.isArray(data)) $scope.routeDirectionInfos = data;
                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10,
                }, {
                    filterDelay : 100,
                    counts : [10, 25, 50, 100],
                    dataset : $scope.routeDirectionInfos
                });
            });
    }

    function switchTag(tag) {
        $scope.currentTag = tag;
        loadAllRouteDirectionInfosByTag($scope.currentTag);
    }

}
