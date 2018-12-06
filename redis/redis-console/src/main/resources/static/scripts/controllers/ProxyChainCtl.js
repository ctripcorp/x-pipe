index_module.controller('ProxyChainCtl',['$rootScope', '$scope', '$window', 'ProxyService', 'ClusterService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, $window, ProxyService, ClusterService, NgTableParams, $stateParams) {

        $scope.dcs, $scope.chains;
        $scope.clusterName = $stateParams.clusterName;

        $scope.switchDc = switchDc;
        $scope.loadChains = loadChains;
        $scope.loadProxyChains = loadProxyChains;
        $scope.gotoProxy = gotoProxy;

        if ($scope.clusterName) {
            loadChains();
        }

        function switchDc(dc) {
            $scope.currentDcName = dc.dcName;
            loadProxyChains($scope.clusterName, dc.dcName);
        }

        function loadChains() {
            ClusterService.findClusterDCs($scope.clusterName)
                .then(function (result) {
                    if (!result || result.length === 0) {
                        $scope.dcs = [];
                        $scope.shards = [];
                        return;
                    }
                    $scope.dcs = result;

                    // TODO [marsqing] do not re-get dc data when switch dc
                    if($scope.dcs && $scope.dcs.length > 0) {
                        $scope.dcs.forEach(function(dc){
                            if(dc.dcName === $stateParams.currentDcName) {
                                $scope.currentDcName = dc.dcName;
                            }
                        });

                        if(!$scope.currentDcName) {
                            ClusterService.load_cluster($stateParams.clusterName).then(function(result) {
                                var cluster = result;
                                var filteredDc = $scope.dcs.filter(function(dc) {
                                    return dc.id !== cluster.activedcId;
                                })
                                if(filteredDc.length > 0) {
                                    $scope.currentDcName = filteredDc[0].dcName;
                                } else {
                                    $scope.currentDcName = $scope.dcs[0].dcName;
                                }

                                loadProxyChains($scope.clusterName, $scope.currentDcName);
                            }, function(result) {
                                $scope.currentDcName = $scope.dcs[0].dcName;
                                loadProxyChains($scope.clusterName, $scope.currentDcName);
                            });
                        } else {
                            loadProxyChains($scope.clusterName, $scope.currentDcName);
                        }
                    }

                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });


        }

        function loadProxyChains(clusterName, dcName) {
            ProxyService.loadAllProxyChainsForDcCluster(dcName, clusterName)
                .then(function (result) {
                    $scope.chains = result;
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                });
        }

        function gotoProxy(proxyDcId, proxyIp) {
            var uri = "/#/proxy/" + proxyIp + "/" + proxyDcId;
            $window.open(uri);
        }

}]);