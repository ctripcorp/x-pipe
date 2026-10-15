angular
    .module('index')
    .controller('KeeperContainerKeeperOverAllCtl', KeeperContainerKeeperOverAllCtl);

function KeeperContainerKeeperOverAllCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                                   toastr, NgTableParams, $interval) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});
    KeeperContainerService.getAllKeepers($stateParams.keeperContainerId).then(function (response) {
        if (Array.isArray(response))  {
            $scope.originData = response
            $scope.tableParams = new NgTableParams({
                page : 1,
                count : 10,
            }, {
                filterDelay: 100,
                counts: [10, 25, 50],
                dataset: $scope.originData
            });
        }
    })

    $scope.getKeeperContainerId = function () {
        return $stateParams.keeperContainerId;
    }

}