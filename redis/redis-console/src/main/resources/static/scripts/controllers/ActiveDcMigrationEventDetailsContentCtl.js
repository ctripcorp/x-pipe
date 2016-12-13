index_module.controller('ActiveDcMigrationEventDetailsContentCtl', ['$rootScope', '$scope', '$window', '$stateParams','$interval', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams,$interval, AppUtil, toastr, NgTableParams, MigrationService, $filters) {
        $scope.migrationCluster = $stateParams.migrationCluster;

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
        
        $scope.continueMigrationCluster = function(eventId, clusterId) {
            MigrationService.continueMigrationCluster(eventId, clusterId).then(
                function(result) {
                toastr.success("操作成功");
                },
                function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        }

        $interval(function() {
            // TODO : refresh contents
        }, 1000);

    }]);