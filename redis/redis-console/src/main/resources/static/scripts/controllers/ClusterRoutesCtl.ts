angular
    .module('index')
    .controller('ClusterRoutesCtl', ClusterRoutesCtl);

ClusterRoutesCtl.$inject = ['$scope', '$stateParams', 'ClusterService', 'toastr', 'AppUtil'];

function ClusterRoutesCtl($scope, $stateParams, ClusterService, toastr, AppUtil) {

    $scope.clusterName = $stateParams.clusterName;
    $scope.currentDcName = $stateParams.dcName;

    $scope.dcs;
    $scope.designatedRoutes=[];
    $scope.usedRoutes=[];
    $scope.defaultRoutes=[];

    $scope.switchDc = switchDc;
    $scope.loadDcClusterRoutes = loadDcClusterRoutes;

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
        ClusterService.getClusterUsedRoutesByDcNameAndClusterName(dcName, clusterName)
             .then(function (result) {
                 $scope.usedRoutes = result;
             }, function (result) {
                 toastr.error(AppUtil.errorMsg(result));
             });
         ClusterService.getClusterDefaultRoutesByDcNameAndClusterName(dcName, clusterName)
              .then(function (result) {
                  $scope.defaultRoutes = result;
              }, function (result) {
                  toastr.error(AppUtil.errorMsg(result));
              });
    }
}
