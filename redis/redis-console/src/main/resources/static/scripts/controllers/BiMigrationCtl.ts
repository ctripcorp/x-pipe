angular
    .module('index')
    .controller('BiMigrationCtl', BiMigrationCtl);

BiMigrationCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'NgTableParams', 'ClusterType', 'ClusterService', 'DcService', 'MigrationService', '$q'];

function BiMigrationCtl($rootScope, $scope, $window, $stateParams, AppUtil, toastr, NgTableParams,
                        ClusterType, ClusterService, DcService, MigrationService, $q) {
    $scope.loading = false;
    $scope.chooseAllClusters = false;
    // data refresh
    ClusterService.findAllClustersByType(ClusterType._values.bi_direction.value)
        .then(data => {
            $scope.clusters = data;
            $scope.tableParams = new NgTableParams({
                page : 1,
                count : 10,
            }, {
                filterDelay:100,
                dataset: $scope.clusters,
            });
        });
    DcService.loadAllDcs()
        .then(data => {
            $scope.selectedDcs = [];
            $scope.allDcs = data
        });
    // fun
    $scope.migrate = migrate;
    $scope.toggleDc = toggleDc;
    $scope.toggleCluster = toggleCluster;
    $scope.toggleAllClusters = toggleAllClusters;

    function allClustersChosen() {
        if (!$scope.clusters) return false;
        return $scope.clusters.every((cluster) => cluster.isChecked);
    }

    function toggleDc(dc) {
        dc.selected = !dc.selected;
    }

    function toggleCluster(cluster) {
        cluster.isChecked = !cluster.isChecked;
        $scope.chooseAllClusters = allClustersChosen();
    }

    function toggleAllClusters() {
        if (!$scope.clusters) return;

        $scope.chooseAllClusters = !$scope.chooseAllClusters;
        $scope.clusters.forEach((cluster) => cluster.isChecked=$scope.chooseAllClusters)
    }

    function migrate() {
        if (!$scope.clusters || !$scope.allDcs) return;
        if ($scope.loading) return;

        $scope.loading = true;

        let checkedClusters = $scope.clusters.filter((cluster) => cluster.isChecked);
        let selectedDcs = $scope.allDcs.filter((dc) => dc.selected);
        MigrationService.syncBiMigration(checkedClusters, selectedDcs)
            .then((resp) => {
                $scope.loading=false;
                if (0 === resp.state) {
                    toastr.success("切换成功");
                } else {
                    toastr.error("切换失败", resp.message);
                }
            }).catch(() => {
                $scope.loading=false;
                toastr.error("切换失败");
        })
    }
}