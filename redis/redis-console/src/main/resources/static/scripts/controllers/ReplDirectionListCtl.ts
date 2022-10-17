angular
    .module('index')
    .controller('ReplDirectionListCtl', ReplDirectionListCtl);

ReplDirectionListCtl.$inject = ['$rootScope', '$scope', 'ReplDirectionService', 'NgTableParams'];

function ReplDirectionListCtl($rootScope, $scope, ReplDirectionService, NgTableParams) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});

    ReplDirectionService.getAllReplDirectionInfos().then(function (response) {
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
}