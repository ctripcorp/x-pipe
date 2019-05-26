index_module.controller('ActiveDcMigrationEventDetailsContentCtl', ['$rootScope', '$scope', '$window', '$stateParams','$interval','$state', 'AppUtil', 'toastr', 'NgTableParams', 'MigrationService','DcService',
    function ($rootScope, $scope, $window, $stateParams,$interval,$state, AppUtil, toastr, NgTableParams, MigrationService,DcService, $filters) {
        $scope.migrationCluster = $stateParams.migrationCluster;
        $scope.currentQueryLog;
        $scope.dcs;

        if($scope.$parent.dcs) {
        	$scope.dcs = $scope.$parent.dcs;
        	init();
        } else {
        	DcService.loadAllDcs().then(function(result) {
        		$scope.dcs = result;
        		init();
        	});
        }
        
        function init() {
        	if($scope.migrationCluster) {
        		loadDetails();
        		if($scope.migrationCluster.migrationShards) {
                    initStatus();
                } else {
                    if($scope.$parent.eventDetails) {
                        $scope.migrationCluster = $scope.$parent.eventDetails[0];
                        initStatus();
                    } else {
                        MigrationService.findEventDetails($scope.$parent.eventId).then(function(result) {
                            $scope.$parent.eventDetails = result;
                            $scope.migrationCluster = $scope.$parent.eventDetails[0];
                            initStatus();
                        });
                    }
                }
        	}
        }
        
        function initStatus() {
        	if($scope.migrationCluster) {
        		$scope.migrationCluster.migrationShards.forEach(function(migrationShard) {
                	if(migrationShard.migrationShard) {
                		if(migrationShard.migrationShard.log) {
                            migrationShard.status = JSON.parse(migrationShard.migrationShard.log);
                        } else {
                            migrationShard.status = {};
                        }	
                	}
                });	
        	}
        }

        var stopInterval;

        function loadDetails(){
            toastr.success("操作成功");
            if ( angular.isDefined(stopInterval) ) return;

            stopInterval = $interval(function() {
            MigrationService.findEventDetails($scope.eventId).then(
                function(result) {
                    $scope.$parent.eventDetails = result;
                    $scope.$parent.eventDetails.forEach(function(migrationCluster) {
                        if(migrationCluster.migrationCluster.id === $scope.migrationCluster.migrationCluster.id) {
                            $scope.migrationCluster = migrationCluster;
                            initStatus();
                            if(migrationCluster.migrationCluster.end){
                            	$interval.cancel(stopInterval);
                            	stopInterval = undefined;
                            }
                        }
                    });
                },
                function(result) {
                });
            }, 1500);        	
        }
        
        
        $scope.continueMigrationCluster = function(eventId, clusterId) {
            MigrationService.continueMigrationCluster(eventId, clusterId).then(
            	loadDetails,
                function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        }
        
        $scope.cancelMigrationCluster = function(eventId, clusterId) {
        	MigrationService.cancelMigrationCluster(eventId, clusterId).then(
        			loadDetails,
                    function(result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }

        $scope.rollbackMigrationCluster = function (eventId, clusterId) {
            MigrationService.rollbackMigrationCluster(eventId, clusterId).then(
            	loadDetails,
                function(result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }
        
        $scope.forcePublishMigrationCluster = function(eventId, clusterId) {
        	MigrationService.forcePublishMigrationCluster(eventId, clusterId).then(
        			loadDetails,
        			function(result) {
                        toastr.error(AppUtil.errorMsg(result));
                    });
        }
        
        $scope.forceEndMigrationCluster = function(eventId, clusterId) {
        	MigrationService.forceEndMigrationCluster(eventId, clusterId).then(
        			loadDetails,
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
        
        $scope.showPublishResult = function(publishInfo) {
        	if(publishInfo) {
        		$scope.publishInfo = publishInfo;
        	}
        	$('#publishLog').modal('show');
        }

        $scope.hidePublishLog = function() {
        	$scope.publishInfo = '';
        	$('#publishLog').modal('hide');
        }
        
    }]);