angular
    .module('index')
    .controller('RouteListCtl', RouteListCtl);

RouteListCtl.$inject = ['$rootScope', '$scope', '$window', 'RouteService', 'NgTableParams', '$stateParams'];

function RouteListCtl($rootScope, $scope, $window, RouteService, NgTableParams, $stateParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['META', 'CONSOLE'];

    $scope.switchTag = switchTag;

    $scope.routeSwitch = routeSwitch;


    showAllRoutes();

    function showAllRoutes() {
        $scope.currentTag = ['META'];
        loadAllRouteByTag($scope.currentTag);
    }

    function loadAllRouteByTag(tag) {
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
    }

    function switchTag(tag) {
        console.log(tag);
        $scope.currentTag = tag;
        loadAllRouteByTag($scope.currentTag);
    }

    function routeSwitch() {
        console.log("11111");
        $('#routeSwitch').modal('show');
    }
}