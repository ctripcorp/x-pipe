angular
    .module('index')
    .controller('ShardListCtl', ShardListCtl);

ShardListCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil',
    'toastr', 'ClusterService','DcService', 'NgTableParams', 'ClusterType'];

function ShardListCtl($rootScope, $scope, $window, $stateParams, AppUtil,
                      toastr, ClusterService, DcService, NgTableParams, ClusterType) {

    $rootScope.currentNav = '1-2';
    $scope.dcs = {};
    $scope.getActiveDcName = getActiveDcName;
    $scope.getTypeName = getTypeName
    $scope.showUnhealthyShardOnly = true;
    $scope.clusterTypes = ClusterType.selectData()

    $scope.sourceClusters = [];
    showClusters();

    DcService.loadAllDcs()
        .then(function(data) {
            for(var i = 0 ; i < data.length; ++i) {
                var dc = data[i];
                $scope.dcs[dc.id] = dc.dcName;
            }
        });

    function getActiveDcName(shard) {
        var clusterType = ClusterType.lookup(shard.clusterType)
        if (clusterType && clusterType.multiActiveDcs) {
            return "-"
        }
        return $scope.dcs[shard.activedcId] || "Unbind";
    }

    function getTypeName(type) {
        var clusterType = ClusterType.lookup(type)
        if (clusterType) return clusterType.name
        else return '未知类型'
    }

    $scope.refresh = function() {
        showClusters();
    }

    function showClusters() {
        if ($scope.showUnhealthyShardOnly) {
            showUnhealthyShards()
        }
        else {
            showAllClusters();
        }
    }

    function showUnhealthyShards() {
        ClusterService.getUnhealthyShards()
            .then(loadTable);
    }

    function showAllClusters() {
        ClusterService.findAllClusters()
            .then(loadTable);
    }

    function loadTable(data) {
        $scope.sourceClusters = data;
        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10
        }, {
            filterDelay:100,
            dataset: $scope.sourceClusters,
        });
    }
}
