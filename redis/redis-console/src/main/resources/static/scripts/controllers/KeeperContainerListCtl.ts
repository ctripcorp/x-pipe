angular
    .module('index')
    .controller('KeeperContainerListCtl', KeeperContainerListCtl);

KeeperContainerListCtl.$inject = ['$rootScope', '$scope', 'KeeperContainerService', 'LogicalBuService', 'NgTableParams'];

function KeeperContainerListCtl($rootScope, $scope, KeeperContainerService, LogicalBuService, NgTableParams) {
    $scope.originData = []
    $scope.logicalBuNameMap = {0: '-'};

    $scope.tableParams = new NgTableParams({}, {});

    LogicalBuService.findAll().then(function (bus) {
        (bus || []).forEach(function (bu) {
            $scope.logicalBuNameMap[bu.id] = bu.name;
        });
        return KeeperContainerService.getAllInfos();
    }).then(function (response) {
        if (Array.isArray(response)) {
            $scope.originData = response.map(function (info) {
                info.logicalBuName = $scope.logicalBuNameMap[info.logicalBuId] || '-';
                return info;
            });
        }

        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10,
        }, {
            filterDelay: 100,
            counts: [10, 25, 50],
            dataset: $scope.originData
        });
    });
}