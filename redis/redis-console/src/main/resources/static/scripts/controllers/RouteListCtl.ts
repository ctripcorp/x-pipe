angular
    .module('index')
    .controller('RouteListCtl', RouteListCtl);

RouteListCtl.$inject = ['$scope', 'RouteService', 'NgTableParams', '$stateParams', 'ClusterType'];

function RouteListCtl($scope, RouteService, NgTableParams, $stateParams, ClusterType) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});
    $scope.tags = ['meta', 'console'];

    $scope.srcDcName = $stateParams.srcDcName;
    $scope.dstDcName = $stateParams.dstDcName;
    $scope.clusterTypes = ClusterType.selectData()
    $scope.getTypeName = getTypeName;

    $scope.switchTag = switchTag;

    showAllRoutes();

    function getTypeName(type) {
        if (null == type || "" == type) return ""
        var clusterType = ClusterType.lookup(type)
        if (clusterType) return clusterType.name
        else return '未知类型'
    }

    function showAllRoutes() {
        $scope.currentTag = ['meta'];
        loadAllRoutesByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }

    function loadAllRoutesByTag(tag, srcDcName, dstDcName) {
        if((srcDcName == 'true') && (dstDcName == 'true')) {
            RouteService.getAllActiveRoutesByTag(tag).then(function (data) {
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
            RouteService.getAllActiveRoutesByTagAndDirection(tag, srcDcName, dstDcName).then(function (data) {
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
        loadAllRoutesByTag($scope.currentTag, $scope.srcDcName, $scope.dstDcName);
    }
}
