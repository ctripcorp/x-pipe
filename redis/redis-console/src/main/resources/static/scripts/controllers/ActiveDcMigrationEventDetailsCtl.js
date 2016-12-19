index_module.controller('ActiveDcMigrationEventDetailsCtl', ['$rootScope', '$scope', '$window', '$stateParams','$state', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams,$state, AppUtil, toastr, NgTableParams, MigrationService, $filters) {
    	
    	$scope.eventId = $stateParams.eventId;
    	
    	$scope.eventDetails;

        if($scope.eventId) {
            init();
        }

    	function init() {
    		MigrationService.findEventDetails($scope.eventId).then(
    			function(result) {
    				$scope.eventDetails = result;
                    $state.go('.details',{migrationCluster : $scope.eventDetails[0]});
    			});
    	}
    }]);