index_module.controller('ActiveDcMigrationEventDetailsCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, $filters) {
    	$scope.eventId = $stateParams.eventId;
    	
    }]);