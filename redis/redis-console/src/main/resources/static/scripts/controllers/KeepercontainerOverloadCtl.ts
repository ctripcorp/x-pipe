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
    $scope.selectAll = false;
    $scope.toggleAll = toggleAll;
    $scope.isChecked = isChecked;

    var OPERATE_TYPE = {
        DETAIL: 'detail',
        MIGRATING: 'migrating',
        STOPPED : 'stopped'
    };
    $scope.operateType = $stateParams.type;
    $scope.migratingKeeperContainers = [];
    $scope.scheduledWork;

    $scope.beginToMigrateOverloadKeeperContainers = beginToMigrateOverloadKeeperContainers;

    KeeperContainerService.getAllOverloadKeepercontainer()
        .then(function (result) {
            if (Array.isArray(result)) $scope.overloadKeeperContainer = result;
            $scope.overloadKeeperContainer.forEach(function(container) {
                switch(container.cause) {
                    case 'BOTH':
                        container.cause = '数据量和流量超载';
                        break;
                    case 'PEER_DATA_OVERLOAD':
                        container.cause = '数据量超载';
                        break;
                    case 'INPUT_FLOW_OVERLOAD':
                        container.cause = '流量超载';
                        break;
                    case 'KEEPER_PAIR_BOTH':
                    case 'KEEPER_PAIR_PEER_DATA_OVERLOAD':
                    case 'KEEPER_PAIR_INPUT_FLOW_OVERLOAD':
                        container.cause = 'keeper对超载';
                        break;
                }
                if (!container.switchActive && !container.keeperPairOverload) {
                    container.result = '迁移主keeper'
                } else if (container.switchActive && !container.keeperPairOverload) {
                    container.result = '主备切换'
                } else if (!container.switchActive && container.keeperPairOverload) {
                    container.result = '迁移备keeper'
                }
                container.time = container.updateTime.substring(0, 19).replace("T", " ");
            });
            $scope.tableParams = new NgTableParams({
                page : 1,
                count : 10,
            }, {
                filterDelay: 100,
                counts: [10, 25, 50],
                dataset: $scope.overloadKeeperContainer
            });
        });


	function beginToMigrateOverloadKeeperContainers() {
        $scope.migratingKeeperContainers = $scope.overloadKeeperContainer.filter(function(keeperContainer){
            return keeperContainer.selected;
        });

        $scope.tableParams = new NgTableParams({}, {});

        $scope.migratingTableParams = new NgTableParams({
            page : 1,
            count : 10,
        }, {
            filterDelay: 100,
            counts: [10, 25, 50],
            dataset: $scope.migratingKeeperContainers
        });

        $scope.operateType = OPERATE_TYPE.MIGRATING;

        KeeperContainerService.beginToMigrateOverloadKeeperContainers.apply(KeeperContainerService, $scope.migratingKeeperContainers)
            .then(result => {
                if(result.message == 'success' ) {
                    toastr.success("迁移成功");
                } else {
                    toastr.error(result.message, "迁移失败");
                }
                getOverloadKeeperContainerMigrationProcess();
                $interval.cancel($scope.scheduledWork);
            });
	}

	function getOverloadKeeperContainerMigrationProcess() {
        if ($scope.operateType == OPERATE_TYPE.MIGRATING) {
            KeeperContainerService.getOverloadKeeperContainerMigrationProcess()
                .then(function (result) {
                    if (result == null) return;
                    $scope.migratingKeeperContainers = result;
                    $scope.migratingTableParams = new NgTableParams({
                        page : 1,
                        count : 10,
                    }, {
                        filterDelay: 100,
                        counts: [10, 25, 50],
                        dataset: $scope.migratingKeeperContainers
                    });
                });
        }
	}

    $scope.scheduledWork = $interval(getOverloadKeeperContainerMigrationProcess, 1000);

    function toggleAll() {
        $scope.selectAll = !$scope.selectAll;
        $scope.overloadKeeperContainer.forEach(function (keeperContainer) {
            keeperContainer.selected = !keeperContainer.selected;
        });
    }

    function isChecked() {
        return $scope.selectAll;
    }
}