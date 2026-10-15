angular
	.module('index')
	.controller('KeeperMigrationCtl', KeeperMigrationCtl);

KeeperMigrationCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'KeeperContainerService',
	'toastr', 'NgTableParams', 'ClusterService', 'RedisService', 'ClusterType'];

function KeeperMigrationCtl($rootScope, $scope, $window, $stateParams, KeeperContainerService,
								   toastr, NgTableParams, ClusterService, RedisService, ClusterType) {

    $scope.keepercontainerId = $stateParams.keepercontainer;
    $scope.srcKeepercontainer = [];
    $scope.allClustersWithSrcKeeperContainer = [];
    $scope.availableKeeperContainers = [];
    $scope.availableMigrationKeeperNums = [];
    $scope.maxMigrationKeeperNum = 0;
    $scope.targetKeeperContainer = {};

    $scope.toggle = toggle;
    $scope.toggleAll = toggleAll;
    $scope.isIndeterminate = isIndeterminate;
    $scope.isChecked = isChecked;

	$scope.preMigrateKeepers = preMigrateKeepers;
	$scope.doMigrateKeepers = doMigrateKeepers;
	$scope.gotoClusterHickwall = gotoClusterHickwall;
	$scope.keeperContainerAddr = keeperContainerAddr;

	init();

	function init() {
	    for (var i = 0; i < 50; i++) {
	        $scope.availableMigrationKeeperNums.push(i + 1);
	    }
	    KeeperContainerService.findKeepercontainerById($scope.keepercontainerId)
	        .then(function (result) {
	            $scope.srcKeepercontainer = result;
                KeeperContainerService.findAvailableKeepersByDcAzOrgAndTag($scope.srcKeepercontainer.dcName,
                                                    $scope.srcKeepercontainer.azName, $scope.srcKeepercontainer.orgName, $scope.srcKeepercontainer.tag)
                    .then(function (result) {
                        $scope.availableKeeperContainers = result.filter(keepercontainer => keepercontainer.id != $scope.srcKeepercontainer.id);
                    });
	        });

        ClusterService.findAllByKeeperContainer($scope.keepercontainerId)
            .then(function (result) {
                $scope.allClustersWithSrcKeeperContainer = result;

                $scope.tableParams = new NgTableParams({
                    page : 1,
                    count : 10,
                }, {
                    filterDelay: 100,
                    counts: [10, 25, 50],
                    dataset: $scope.allClustersWithSrcKeeperContainer,
                });
            });
	}

	/**
	 * 容器化后 hostname 才是 KeeperContainer 的稳定标识，ip 可能随 pod 重建变化甚至为空，
	 * 因此优先展示 hostname，没有 hostname 的物理机再回退到 ip
	 */
	function keeperContainerAddr(keeperContainer) {
	    if (!keeperContainer) {
	        return '';
	    }
	    var host = keeperContainer.keepercontainerHostName || keeperContainer.ip;
	    return host ? host + ':' + keeperContainer.port : '';
	}

	function preMigrateKeepers() {
	    $('#migrateKeepersConfirm').modal('show');
	}

	function doMigrateKeepers() {
        var selectedClusters = $scope.allClustersWithSrcKeeperContainer.filter(function(cluster){
            return cluster.selected && (cluster.targetDc !== "-");
        });

        $scope.availableKeeperContainers.forEach(function(keeperContainer){
            // 与下拉项 value 保持同一套展示规则（hostname 优先），否则选中项匹配不上
            if (keeperContainerAddr(keeperContainer) == $scope.targetKeeperContainer) {
                $scope.targetKeeperContainer = keeperContainer;
            }
        });

        RedisService.migrateKeepers($scope.maxMigrationKeeperNum, $scope.targetKeeperContainer, $scope.srcKeepercontainer, selectedClusters)
            .then(function (result) {
                $('#migrateKeepersConfirm').modal('hide');
                toastr.success("迁移成功");
                $window.location.href = "/#/keepercontainers";
                $window.location.reload()
            });
	}

    function toggle (cluster) {
        cluster.selected = !cluster.selected;
    };

    function isIndeterminate() {
        var selectedClusters = $scope.allClustersWithSrcKeeperContainer.filter(function(cluster){
            return cluster.selected;
        });
        return (selectedClusters.length !== $scope.allClustersWithSrcKeeperContainer.length) &&
                (selectedClusters.length > 0);
    };

    function isChecked() {
        var selectedClusters = $scope.allClustersWithSrcKeeperContainer.filter(function(cluster){
            return cluster.selected;
        });
        return (selectedClusters.length === $scope.allClustersWithSrcKeeperContainer.length) &&
                (selectedClusters.length !== 0);
    };

    function toggleAll() {
        if($scope.isIndeterminate()) {
            $scope.allClustersWithSrcKeeperContainer.forEach(function(cluster){
            cluster.selected = true;
            });
        } else {
            $scope.allClustersWithSrcKeeperContainer.forEach(function(cluster){
            cluster.selected = !cluster.selected;
            });
        }
    };

    function gotoClusterHickwall(clusterName) {
        ClusterService.getClusterHickwallAddr(clusterName, ClusterType.lookup('one_way').value).then(function(result) {
            if(result != null && result.state === 0) {
                $window.open(result.message, '_blank');
            }
        });
    }
}