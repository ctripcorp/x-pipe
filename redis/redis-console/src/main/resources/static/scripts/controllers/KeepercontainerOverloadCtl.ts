angular
	.module('index')
	.controller('KeepercontainerOverloadCtl', KeepercontainerOverloadCtl);

KeepercontainerOverloadCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'KeeperContainerService',
	'toastr', 'NgTableParams', '$interval'];

function KeepercontainerOverloadCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
								   toastr, NgTableParams, $interval) {
    $scope.overloadKeeperContainer = [];
    $scope.tableParams = new NgTableParams({}, {});
    $scope.migratingTableParams = new NgTableParams({}, {});

    var OPERATE_TYPE = {
        DETAIL: 'detail',
        MIGRATING: 'migrating',
        STOPPED : 'stopped'
    };
    $scope.operateType = $stateParams.type;
    $scope.migratingKeeperContainers = [];
    $scope.scheduledWork;

	init();

	function init() {
	    KeeperContainerService.getAllOverloadKeepercontainer()
	        .then(function (result) {
	            $scope.overloadKeeperContainer = result;

                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10,
                }, {
                    filterDelay: 100,
                    counts: [10, 25, 50],
                    dataset: $scope.overloadKeeperContainer
                });
	        });
	}

	function beginToMigrateOverloadKeeperContainers() {
        $scope.migratingKeeperContainers = $scope.overloadKeeperContainer.filter(function(keeperContainer){
            return keeperContainer;
        });

        $scope.migratingTableParams = new NgTableParams({
            page : 1,
            count : 10,
        }, {
            filterDelay: 100,
            counts: [10, 25, 50],
            dataset: $scope.migratingKeeperContainers
        });

        KeeperContainerService.beginToMigrateOverloadKeeperContainers.apply(KeeperContainerService, $scope.migratingKeeperContainers)
            .then(result => {
                $scope.operateType = OPERATE_TYPE.MIGRATING;
                $scope.scheduledWork = $interval(getOverloadKeeperContainerMigrationProcess, 10000);
            });
	}

	function getOverloadKeeperContainerMigrationProcess() {
        KeeperContainerService.getOverloadKeeperContainerMigrationProcess()
            .then(function (result) {
                if (result == null) return;
	            $scope.migratingKeeperContainers = result;
	        });
	}

	function stopToMigrateOverloadKeeperContainers() {
	    if ($scope.operateType == OPERATE_TYPE.STOPPED) return;
	    KeeperContainerService.stopToMigrateOverloadKeeperContainers();
	    $interval.cancel($scope.scheduledWork);
	    $scope.operateType = OPERATE_TYPE.STOPPED;
	}
}