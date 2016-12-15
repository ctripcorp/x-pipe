index_module.controller('ActiveDcMigrationEventListCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $filters) {

		$scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            getData : function(params) {
                return MigrationService.findAll();
            }
        });
    }]);