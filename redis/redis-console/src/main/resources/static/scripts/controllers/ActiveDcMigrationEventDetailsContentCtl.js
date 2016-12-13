index_module.controller('ActiveDcMigrationEventDetailsContentCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams, MigrationService, $filters) {

        $scope.migrationCluster = $stateParams.migrationCluster;
        $scope.steps;

        if($scope.migrationCluster) {
            if($scope.migrationCluster.migrationShards) {
                initStatus();
            }
        }

        function initStatus() {
            $scope.migrationCluster.migrationShards.forEach(function(migrationShard) {
                if(migrationShard.migrationShard.log) {
                    migrationShard.status = JSON.parse(migrationShard.migrationShard.log);
                } else {
                    migrationShard.status = {};
                }
            });
        }
        
    }]);