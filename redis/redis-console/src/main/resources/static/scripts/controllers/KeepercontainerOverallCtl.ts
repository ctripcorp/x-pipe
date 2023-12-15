angular
    .module('index')
    .controller('KeepercontainerOverallCtl', KeepercontainerOverallCtl);

function KeepercontainerOverallCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                                    toastr, NgTableParams, $interval) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});
    KeeperContainerService.getAllKeepercontainerUsedInfo().then(function (response) {
        if (Array.isArray(response))  {
            response.forEach(function (keeperContainerInfo) {
                if (keeperContainerInfo.keeperIp === $stateParams.keepercontainerIp) {
                    $scope.originData = Object.entries(keeperContainerInfo.detailInfo).map(function(item) {
                        var key = JSON.parse(item[0]
                            .replace(/'/g, '\"')
                            .replace(/(\w+)\s*=/g, '\"$1\":')
                            .replace(/\w+\{/g, '{')
                        );
                        return {
                            key: key,
                            value: item[1]
                        };
                    });
                }
            })
            console.log($scope.originData)
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

    $scope.getIp = function () {
        return $stateParams.keepercontainerIp;
    }

    $scope.getDc = function () {
        return $scope.originData[0].key.dcId;
    }
}