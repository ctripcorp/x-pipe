index_module.controller('ActiveDcMigrationEventDetailsCtl', ['$rootScope', '$scope', '$window', '$stateParams','$state', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService','DcService',
    function ($rootScope, $scope, $window, $stateParams,$state, AppUtil, toastr, NgTableParams, MigrationService,DcService, $filters) {
    	
    	$scope.eventId = $stateParams.eventId;
    	
    	$scope.eventDetails;
    	$scope.dcs;

        if($scope.eventId) {
        	DcService.loadAllDcs().then(function(result) {
            	$scope.dcs = result;
            });
        	
            MigrationService.findEventDetails($scope.eventId).then(
                function(result) {
                    $scope.eventDetails = result;
                    $state.go('.details',{migrationCluster : $scope.eventDetails[0]});
                });
        }
        
    }]);