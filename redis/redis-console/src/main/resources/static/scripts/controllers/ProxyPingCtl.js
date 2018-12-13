index_module.controller('ProxyPingCtl',['$rootScope', '$scope', 'toastr', 'AppUtil', '$window', 'ProxyPingService', 'DcService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, toastr, AppUtil, $window, ProxyPingService, DcService, NgTableParams, $stateParams) {

        $scope.collectors = [];

        $scope.loadProxyCollectors = loadProxyCollectors;
        $scope.prettyJson = prettyJson;
        $scope.getAllDcs = getAllDcs;
        $scope.switchDc = switchDc;
        $scope.toDate = toDate;
        $scope.gotoProxyPingHickwall = gotoProxyPingHickwall;

        getAllDcs();

        function switchDc(dc) {
            $scope.currentDcId = dc.dcName;
            loadProxyCollectors($scope.currentDcId);
        }

        function loadProxyCollectors(dcName) {
            ProxyPingService.getDcBasedCollectors(dcName)
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

        function gotoProxyPingHickwall() {
            ProxyPingService.getHickwallAddr()
                .then(function(result) {
                    if(result.addr) {
                        $window.open(result.addr, '_blank');
                    }
                });
        }

    }]);