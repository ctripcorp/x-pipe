index_module.controller('KeeperContainerListCtl',['$rootScope', '$scope', 'KeeperContainerService', 'NgTableParams',
    function ($rootScope, $scope, KeeperContainerService, NgTableParams) {
        $scope.originData = []

        $scope.tableParams = new NgTableParams({}, {});

        KeeperContainerService.getAllInfos().then(function (response) {
            if (Array.isArray(response)) $scope.originData = response

            $scope.tableParams = new NgTableParams({
                page : 1,
                count : 10,
            }, {
                filterDelay: 100,
                counts: [10, 25, 50],
                dataset: $scope.originData
            });
        })

    }]);