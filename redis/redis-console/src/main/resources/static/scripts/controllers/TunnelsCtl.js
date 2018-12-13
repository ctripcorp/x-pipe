index_module.controller('TunnelsCtl',['$rootScope', '$scope', 'toastr', 'AppUtil', '$window', 'TunnelService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, toastr, AppUtil, $window, TunnelService, NgTableParams, $stateParams) {

        $scope.dcId = $stateParams.dcId;
        $scope.proxyIp = $stateParams.proxyIp;
        $scope.tunnels = [];

        $scope.loadTunnels = loadTunnels;
        $scope.prettyJson = syntaxHighlight;
        $scope.gotoChain = gotoChain;
        $scope.prettyPrint = prettyPrint;

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

        function prettyPrint(metrics) {
            result = "";
            metrics.forEach(function(metric){
                result += metric.metricType + ":" + metric.value + ";  ";
            });
            return result;
        }

        function prettyJson(obj) {
            return JSON.stringify(obj, null, '\t');
        }

        function syntaxHighlight(obj) {
            var json = JSON.stringify(obj, null, '\t');
            return json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }

        function gotoChain(backupDcId, clusterId) {
            var uri = "/#/chain/" + clusterId + "/" + backupDcId;
            $window.open(uri);
        }

    }]);