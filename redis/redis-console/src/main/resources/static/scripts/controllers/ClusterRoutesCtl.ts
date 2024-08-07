angular
    .module('index')
    .controller('ClusterRoutesCtl', ClusterRoutesCtl);

ClusterRoutesCtl.$inject = ['$scope', '$stateParams', 'ClusterService', 'toastr', 'AppUtil', 'ClusterType'];

function ClusterRoutesCtl($scope, $stateParams, ClusterService, toastr, AppUtil, ClusterType) {

    $scope.clusterName = $stateParams.clusterName;
    $scope.currentDcName = $stateParams.dcName;

    $scope.dcs;
    $scope.designatedRoutes=[];
    $scope.usedRoutes=[];
    $scope.defaultRoutes=[];

    $scope.switchDc = switchDc;
    $scope.loadDcClusterRoutes = loadDcClusterRoutes;

    $scope.clusterTypes = ClusterType.selectData()
    $scope.getTypeName = getTypeName;

    if ($scope.clusterName) {
        loadClusterRoutes();
    }

    function switchDc(dc) {
        $scope.currentDcName = dc.dcName;
        loadDcClusterRoutes($scope.currentDcName, $scope.clusterName);
    }

    function getTypeName(type) {
        if (null == type || "" == type) return ""
        var clusterType = ClusterType.lookup(type)
        if (clusterType) return clusterType.name
        else return '未知类型'
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

    function loadDcClusterRoutes(srcDcName, clusterName) {
        ClusterService.getClusterDesignatedRoutesBySrcDcNameAndClusterName(srcDcName, clusterName)
            .then(function (result) {
                $scope.designatedRoutes = result;
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });

        ClusterService.getClusterUsedRoutesBySrcDcNameAndClusterName(srcDcName, clusterName)
             .then(function (result) {
                 $scope.usedRoutes = result;
             }, function (result) {
                 toastr.error(AppUtil.errorMsg(result));
             });

         ClusterService.getClusterDefaultRoutesBySrcDcNameAndClusterName(srcDcName, clusterName)
              .then(function (result) {
                  $scope.defaultRoutes = result;
              }, function (result) {
                  toastr.error(AppUtil.errorMsg(result));
              });
    }
}
