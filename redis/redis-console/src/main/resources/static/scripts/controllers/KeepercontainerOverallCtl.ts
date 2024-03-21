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
                    $scope.updateTime = keeperContainerInfo.updateTime;
                    $scope.originData = Object.entries(keeperContainerInfo.detailInfo).map(function(item) {
                        var key = item[0].split(':');
                        return {
                            key: {
                                dcName: key[0],
                                clusterName: key[1],
                                shardName: key[2],
                                active: key[3],
                                port: key[4]
                            },
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

    $scope.getIpDC = function () {
        return $stateParams.keepercontainerIp;
    }

    $scope.getDc = function () {
        return $scope.originData[0].key.dcName;
    }

    $scope.getTime = function (){
        return $scope.updateTime.substring(0, 19).replace("T", " ");
    }
}