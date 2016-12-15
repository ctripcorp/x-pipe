index_module.controller('ActiveDcMigrationEventDetailsContentCtl', ['$rootScope', '$scope', '$window', '$stateParams','$interval','$state', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams,$interval,$state, AppUtil, toastr, NgTableParams, MigrationService, $filters) {
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

                    $interval(function() {
                    MigrationService.findEventDetails($scope.eventId).then(
                        function(result) {
                            $scope.$parent.eventDetails = result;
                            $scope.$parent.eventDetails.forEach(function(migrationCluster) {
                                if(migrationCluster.migrationCluster.id == $scope.migrationCluster.migrationCluster.id) {
                                    $scope.migrationCluster = migrationCluster;
                                    initStatus();
                                }
                            });
                        },
                        function(result) {
                        });
                    }, 1000, 10);
                },
                function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        }

    }]);