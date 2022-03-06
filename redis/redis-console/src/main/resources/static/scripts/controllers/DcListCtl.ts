angular
    .module('index')
    .controller('DcListCtl', DcListCtl);

DcListCtl.$inject = ['$rootScope', '$scope', 'DcService', 'NgTableParams', '$stateParams', 'ClusterType'];

function DcListCtl($rootScope, $scope, DcService, NgTableParams, $stateParams, ClusterType) {

    $scope.dcs = {};
    $scope.dcName = $stateParams.dcName;
    $scope.clusterTypes = ClusterType.selectData();
    $scope.clusterType = "";
    $scope.showAllDcs = showAllDcs;
    $scope.sourceDcs=[];

    DcService.loadAllDcs().then(function (data) {
        for (var i = 0; i < data.length; ++i) {
            var dc = data[i];
            $scope.dcs[dc.id] = dc;
        }
    });

    DcService.findAllDcsRichInfo().then(function (data) {
        $scope.sourceDcs = data;
        showAllDcs();
    });

    function showAllDcs() {
        var filtered_data = [];
        for (var i = 0; i < $scope.sourceDcs.length; i++) {
            var dc = $scope.sourceDcs[i];
            var clusterTypeInfos = dc.clusterTypes;
            for (var j = 0; j < clusterTypeInfos.length; j++) {
                var clusterTypeInfo = clusterTypeInfos[j];
                if (clusterTypeInfo.clusterType.toLowerCase() === $scope.clusterType.toLowerCase()) {
                    filtered_data.push(clusterTypeInfo);
                }
            }
        }

        $scope.tableParams = new NgTableParams({
            page: $rootScope.historyPage,
            count: 10
        }, {
            filterDelay: 100,
            dataset: filtered_data
        });

    }
}