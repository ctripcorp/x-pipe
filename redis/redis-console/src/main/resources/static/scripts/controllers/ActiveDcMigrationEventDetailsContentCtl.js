index_module.controller('ActiveDcMigrationEventDetailsContentCtl', ['$rootScope', '$scope', '$window', '$stateParams','$interval','$state', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService',
    function ($rootScope, $scope, $window, $stateParams,$interval,$state, AppUtil, toastr, NgTableParams, MigrationService, $filters) {
        $scope.migrationCluster = $stateParams.migrationCluster;
        $scope.currentQueryLog;

        if($scope.migrationCluster) {
            if($scope.migrationCluster.migrationShards) {
                initStatus();
            } else {
                if($scope.$parent.eventDetails) {
                    $scope.migrationCluster = $scope.$parent.eventDetails[0];
                } else {
                    MigrationService.findEventDetails($scope.$parent.eventId).then(function(result) {
                        $scope.$parent.eventDetails = result;
                        $scope.migrationCluster = $scope.$parent.eventDetails[0];
                        initStatus();
                    });
                }
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

        $scope.showLog = function(step) {
            if(step) {
                if(step.true) {
                    $scope.currentQueryLog = step.true;
                } else if (step.false) {
                    $scope.currentQueryLog = step.false;
                }
                $('#log').modal('show');
            }
        }

        $scope.hideLog = function() {
            $scope.currentQueryLog = '';
            $('#log').modal('hide');
        }

    }]);