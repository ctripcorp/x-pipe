index_module.controller('ProxyCollectorCtl',['$rootScope', '$scope', 'toastr', 'AppUtil', '$window', 'ProxyCollectorService', 'DcService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, toastr, AppUtil, $window, ProxyCollectorService, DcService, NgTableParams, $stateParams) {

        $scope.collectors = [];

        $scope.loadProxyCollectors = loadProxyCollectors;
        $scope.prettyJson = prettyJson;
        $scope.getAllDcs = getAllDcs;
        $scope.switchDc = switchDc;
        $scope.toDate = toDate;

        getAllDcs();

        // if ($scope.currentDcId) {
        //     loadProxyCollectors($scope.currentDcId);
        // } //else {
        //     $scope.currentDcId = $scope.dcs[0].dcName;
        //     loadProxyCollectors($scope.currentDcId);
        // }

        function switchDc(dc) {
            $scope.currentDcId = dc.dcName;
            loadProxyCollectors($scope.currentDcId);
        }

        function loadProxyCollectors(dcName) {
            ProxyCollectorService.getDcBasedCollectors(dcName)
                .then(function (result) {
                    $scope.collectors = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }


        function prettyJson(obj) {
            return JSON.stringify(obj);
        }

        function toDate(timestamp) {
            return new Date(timestamp);
        }

        function getAllDcs() {
            DcService.loadAllDcs()
                .then(function (result) {
                    $scope.dcs = result;
                    $scope.loadProxyCollectors($scope.dcs[0].dcName);
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }

    }]);