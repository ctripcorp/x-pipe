angular
    .module('index')
    .controller('SearchShardCtl', SearchShardCtl);

function SearchShardCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
                        toastr, NgTableParams, $interval, ShardService) {

    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});

    $scope.searchShard = function () {
        $scope.originData = []
        $scope.tableParams = new NgTableParams({}, {});
        if ($scope.replId !== null && $scope.replId !== "" && $scope.replId !== undefined) {
            if (/^\d+$/.test($scope.replId)) {
                ShardService.findAllByReplId($scope.replId).then(function (response) {
                    if (response.state === 0) {
                        $scope.originData = response.payload
                        $scope.tableParams = new NgTableParams({
                            page : 1,
                            count : 10,
                        }, {
                            filterDelay: 100,
                            counts: [10, 25, 50],
                            dataset: $scope.originData
                        });
                    } else {
                        alert(response.message);
                    }
                })
            } else {
                alert("请输入纯数字！");
            }
        }

        if ($scope.shardName !== null && $scope.shardName !== "" && $scope.shardName !== undefined) {
            ShardService.findAllByShardName($scope.shardName).then(function (response) {
                if (response.state === 0) {
                    response.payload.forEach(item => {
                        if ($scope.originData.length === 0 || $scope.originData[0].shardId !== item.shardId) {
                            $scope.originData.push(item);
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
                } else {
                    alert(response.message);
                }
            })
        }
    }

}