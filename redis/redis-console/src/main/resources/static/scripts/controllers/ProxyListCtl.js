index_module.controller('ProxyListCtl',['$rootScope', '$scope', '$window', 'ProxyService', 'NgTableParams', '$stateParams',
    function ($rootScope, $scope, $window, ProxyService,NgTableParams, $stateParams) {

        $scope.proxies = {};
        $scope.proxyIp = $stateParams.proxyIp;
        $scope.getToTrafficHickWall = getToTrafficHickWall;

        var sourceProxies = [], copiedProxies = [];

        ProxyService.getAllProxyInfo().then(function (data) {
            for (var i = 0; i < data.length; ++i){
                var proxy = data[i];
                $scope.proxies[proxy.ip] = proxy;
            }
        });

        showAllProxies();

        function showAllProxies() {
            ProxyService.getAllProxyInfo().then(function (data) {
                sourceProxies = data;
                copiedProxies = _.clone(sourceProxies);
                $scope.tableParams = new NgTableParams({
                    page: $rootScope.historyPage,
                    count: 10
                }, {
                    filterDelay: 100,
                    getData: function (params) {
                        $rootScope.historyPage = params.page();
                        var filter_text = params.filter().proxyIp;
                        if (filter_text) {
                            var filtered_data = [];
                            for (var i = 0; i < sourceProxies.length; i++) {
                                var proxy = sourceProxies[i];
                                if (proxy.proxyIp.search(filter_text) !== -1) {
                                    filtered_data.push(proxy);
                                }
                            }
                            copiedProxies = filtered_data;
                        } else {
                            copiedProxies = sourceProxies;
                        }

                        params.total(copiedProxies.length);
                        return copiedProxies.slice((params.page() - 1) * params.count(), params.page() * params.count());
                    }
                });
            });
        }

        function getToTrafficHickWall(host, port) {
            ProxyService.getProxyTrafficHickwall(host, port)
                .then(function(result) {
                    if(result.addr) {
                        $window.open(result.addr, '_blank');
                    }
                });
        }


}]);