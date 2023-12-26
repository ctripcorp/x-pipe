angular
    .module('index')
    .controller('KeepercontainerUsedInfoCtl', KeepercontainerUsedInfoCtl);

function KeepercontainerUsedInfoCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                                    toastr, NgTableParams, $interval) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});
    KeeperContainerService.getAllKeepercontainerUsedInfo().then(function (response) {
        console.log(response);
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
        KeeperContainerService.getKeepercontainerFullSynchronizationTime().then(function(response) {
            $scope.fullSynchronizationTime = response.message;
        })
    })

    $scope.getKeepercontainerFullSynchronizationTime = function () {
        return $scope.fullSynchronizationTime;
    }

}