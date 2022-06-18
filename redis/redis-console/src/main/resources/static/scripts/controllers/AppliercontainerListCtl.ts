angular
    .module('index')
    .controller('AppliercontainerListCtl', AppliercontainerListCtl);

AppliercontainerListCtl.$inject = ['$rootScope', '$scope', 'AppliercontainerService', 'NgTableParams'];

function AppliercontainerListCtl($rootScope, $scope, AppliercontainerService, NgTableParams) {
    $scope.originData = []

    $scope.tableParams = new NgTableParams({}, {});

    AppliercontainerService.getAllActiveAppliercontainerInfos().then(function (response) {
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