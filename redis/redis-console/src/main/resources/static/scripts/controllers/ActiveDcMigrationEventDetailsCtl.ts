angular
    .module('index')
    .controller('ActiveDcMigrationEventDetailsCtl', ActiveDcMigrationEventDetailsCtl);

ActiveDcMigrationEventDetailsCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams','$state', 'AppUtil',
    'toastr', 'NgTableParams', 'MigrationService','DcService'];

function ActiveDcMigrationEventDetailsCtl($rootScope, $scope, $window, $stateParams,$state, AppUtil,
                                          toastr, NgTableParams, MigrationService,DcService, $filters) {

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
}