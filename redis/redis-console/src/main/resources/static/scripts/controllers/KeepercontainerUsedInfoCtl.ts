angular
    .module('index')
    .controller('KeepercontainerUsedInfoCtl', KeepercontainerUsedInfoCtl);

function KeepercontainerUsedInfoCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                                    toastr, NgTableParams, $interval) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});
    KeeperContainerService.getAllKeepercontainerUsedInfo().then(function (response) {
        if (Array.isArray(response))  {
            $scope.originData = response;

            KeeperContainerService.getAllInfos().then(function (anotherResponse) {
                if (Array.isArray(anotherResponse)) {
                    Promise.all([response, anotherResponse]).then(function (responses) {
                        var originData = responses[0];
                        var anotherData = responses[1];

                        originData.forEach(function (row) {
                            var matchingData = anotherData.find(function (d) {
                                return d.addr.host === row.keeperIp;
                            });
                            if (matchingData) {
                                Object.assign(row, matchingData);
                            }
                            row.activeRedisUsedMemoryPercentage = $scope.getActivePercentage(row.activeRedisUsedMemory, row.totalRedisUsedMemory)*100;
                            row.activeRedisUsedMemoryStandardPercentage = $scope.getActivePercentage(row.activeRedisUsedMemory, row.redisUsedMemoryStandard)*100;
                            row.activeInputFlowPercentage = $scope.getActivePercentage(row.activeInputFlow, row.totalInputFlow)*100;
                            row.activeInputFlowStandardPercentage = $scope.getActivePercentage(row.activeInputFlow, row.inputFlowStandard)*100;
                            if ($scope.lastUpdateTime == null || row.updateTime > $scope.lastUpdateTime) {
                                $scope.lastUpdateTime = row.updateTime;
                            }
                        });
                    });
                }
            });
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

    $scope.getLastUpdateTime = function () {
        if ($scope.lastUpdateTime == null) {
            return '';
        }
        return $scope.lastUpdateTime.substring(0, 19).replace("T", " ");
    }

    $scope.getActivePercentage = function (active, total) {
        if (total === 0) {
            return 0;
        }
        return active/total;
    }

}