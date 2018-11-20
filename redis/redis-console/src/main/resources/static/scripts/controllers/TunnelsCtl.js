index_module.controller('TunnelsCtl',['$rootScope', '$scope', 'toastr', 'AppUtil', '$window', 'TunnelService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, toastr, AppUtil, $window, TunnelService, NgTableParams, $stateParams) {

        $scope.dcId = $stateParams.dcId;
        $scope.proxyIp = $stateParams.proxyIp;
        $scope.tunnels = [];

        $scope.loadTunnels = loadTunnels;
        $scope.prettyJson = prettyJson;
        $scope.gotoChain = gotoChain;

        if ($scope.dcId && $scope.proxyIp) {
            loadTunnels($scope.dcId, $scope.proxyIp);
        }

        function loadTunnels(dcName, ip) {
            TunnelService.getAllTunnels(dcName, ip)
                .then(function (result) {
                    $scope.tunnels = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }

        function prettyJson(obj) {
            return JSON.stringify(obj);
        }

        function gotoChain(backupDcId, clusterId) {
            var uri = "/#/chain/" + clusterId + "/" + backupDcId;
            $window.open(uri);
        }
    }]);