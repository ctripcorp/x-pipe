index_module.controller('ActiveDcMigrationEventDetailsCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $filters) {
    	
    	$scope.eventId = $stateParams.eventId;
    	
    	$scope.eventDetails;

    	init();

    	function init() {
    		MigrationService.findEventDetails($scope.eventId).then(
    			function(result) {
    				$scope.eventDetails = result;
    			});
    	}
    }]);