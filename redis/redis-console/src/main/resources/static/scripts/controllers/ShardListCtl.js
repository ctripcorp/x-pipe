index_module.controller('ShardListCtl', ['$rootScope', '$scope', '$window', '$stateParams', 'AppUtil', 'toastr', 'ClusterService','DcService', 'NgTableParams',
    function ($rootScope, $scope, $window, $stateParams, AppUtil, toastr, ClusterService, DcService, NgTableParams, $filters) {

        $rootScope.currentNav = '1-2';
        $scope.dcs = {};
        $scope.getDcName = getDcName;
        $scope.showUnhealthyShardOnly = true;

        $scope.sourceClusters = [];
        showClusters();

        DcService.loadAllDcs()
            .then(function(data) {
                for(var i = 0 ; i < data.length; ++i) {
                    var dc = data[i];
                    $scope.dcs[dc.id] = dc.dcName;
                }
            });

        function getDcName(dcId) {
            return $scope.dcs[dcId] || "Unbind";
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


    }]);
