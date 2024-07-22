angular
    .module('index')
    .controller('BiMigrationEventListCtl', BiMigrationEventListCtl);

BiMigrationEventListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'NgTableParams', 'MigrationService', '$q'];

function BiMigrationEventListCtl($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $q) {
    MigrationService.findAllBiMigration().then(data => {
        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            filterDelay:100,
            dataset: data,
        });
    })

}