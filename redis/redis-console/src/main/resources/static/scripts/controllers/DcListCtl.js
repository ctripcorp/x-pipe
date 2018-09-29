index_module.controller('DcListCtl',['$rootScope', '$scope', 'DcService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, DcService,NgTableParams, $stateParams) {

        $scope.dcs = {};
        $scope.dcName = $stateParams.dcName;

        var sourceDcs = [], copedDcs = [];

        DcService.loadAllDcs().then(function (data) {
            for (var i = 0; i < data.length; ++i){
                var dc = data[i];
                $scope.dcs[dc.id] = dc;
            }
        });

        showAllDcs();

        function showAllDcs() {
            DcService.findAllDcsRichInfo().then(function (data) {
                sourceDcs = data;
                copedDcs = _.clone(sourceDcs);
                $scope.tableParams = new NgTableParams({
                    page: $rootScope.historyPage,
                    count: 10
                }, {
                    filterDelay: 100,
                    getData: function (params) {
                        $rootScope.historyPage = params.page();
                        var filter_text = params.filter().dcName;
                        if (filter_text) {
                            var filtered_data = [];
                            for (var i = 0; i < sourceDcs.length; i++) {
                                var dc = sourceDcs[i];
                                if (dc.dcName.search(filter_text) !== -1) {
                                    filtered_data.push(dc);
                                }
                            }
                            copedDcs = filtered_data;
                        } else {
                            copedDcs = sourceDcs;
                        }

                        params.total(copedDcs.length);
                        return copedDcs.slice((params.page() - 1) * params.count(), params.page() * params.count());
                    }
                });
            });
        }


}]);