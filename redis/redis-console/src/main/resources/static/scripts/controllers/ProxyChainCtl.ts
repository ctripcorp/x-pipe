angular
    .module('index')
    .controller('ProxyChainCtl', ProxyChainCtl);

ProxyChainCtl.$inject = ['$rootScope', '$scope', '$window', 'AppUtil',
    'toastr', 'ProxyService', 'ClusterService', 'NgTableParams', '$stateParams'];

function ProxyChainCtl($rootScope, $scope, $window, AppUtil, toastr, ProxyService, ClusterService, NgTableParams, $stateParams) {

    $scope.dcs, $scope.chains;
    $scope.clusterName = $stateParams.clusterName;
    $scope.metrics = {};

    $scope.switchDc = switchDc;
    $scope.loadChains = loadChains;
    $scope.loadProxyChains = loadProxyChains;
    $scope.gotoProxy = gotoProxy;
    $scope.getMetricHickwalls = getMetricHickwalls;
    $scope.gotoHickwallWebSite = gotoHickwallWebSite;
    $scope.closeChain = closeChain;
    $scope.preCloseChain = preCloseChain;

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

                // TODO [nick] do not re-get dc data when switch dc
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
                for (var shardId in result.toJSON()) {
                    var shardChains = result[shardId];
                    shardChains.forEach(function (chain) {
                        getMetricHickwalls(clusterName, shardId)
                            .then(function (value) { chain.metrics = value });
                     });
                }
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function gotoProxy(proxyDcId, proxyIp) {
        var uri = "/#/proxy/" + proxyIp + "/" + proxyDcId;
        $window.open(uri);
    }

    function getMetricHickwalls(clusterId, shardId) {
        return ProxyService.getProxyChainHickwall(clusterId, shardId);
    }

    function gotoHickwallWebSite(addr) {
        $window.open(addr, '_blank');
    }

    function preCloseChain(chain) {
        $scope.chainToClose = chain;
        $('#deleteChainConfirm').modal('show');
    }

    function closeChain() {
        ProxyService.closeProxyChain($scope.chainToClose)
            .then(function (result) {
                $('#deleteChainConfirm').modal('hide');
                if(result.state === 0) {
                    toastr.success('删除成功');
                } else {
                    toastr.error(AppUtil.errorMsg(result), '删除失败');
                }
                setTimeout(function () {
                    // TODO [nick] reload ng-table instead of reload window
                    $window.location.reload();
                },1000);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '删除失败');
            })
    }
}
