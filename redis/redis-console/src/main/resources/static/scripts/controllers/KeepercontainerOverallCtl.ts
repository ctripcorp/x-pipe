angular
    .module('index')
    .controller('KeepercontainerOverallCtl', KeepercontainerOverallCtl);

function KeepercontainerOverallCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                                    toastr, NgTableParams, $interval) {
    $scope.originData = []

    $scope.keeperContainerAddress = '';
    $scope.dcName = '';

    $scope.tableParams = new NgTableParams({}, {});
    KeeperContainerService.getAllKeepercontainerUsedInfo().then(function (response) {
        if (Array.isArray(response))  {
            // $stateParams 取到的是字符串，keeperContainerId 是数字，需先转换再比较
            var keeperContainerId = Number($stateParams.keeperContainerId);
            response.forEach(function (keeperContainerInfo) {
                if (keeperContainerInfo.keeperContainerId === keeperContainerId) {
                    $scope.updateTime = keeperContainerInfo.updateTime;
                    // hostname 优先，与列表页、用量页口径一致
                    $scope.keeperContainerAddress = keeperContainerInfo.hostName || keeperContainerInfo.keeperIp;
                    $scope.dcName = keeperContainerInfo.dcName;
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

    $scope.getAddress = function () {
        return $scope.keeperContainerAddress;
    }

    $scope.getDc = function () {
        return $scope.dcName;
    }

    $scope.getTime = function (){
        if ($scope.updateTime == null) {
            return '';
        }
        return $scope.updateTime.substring(0, 19).replace("T", " ");
    }
}