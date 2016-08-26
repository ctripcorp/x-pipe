index_module.config(function($stateProvider, $urlRouterProvider) {

    $stateProvider
        .state('cluster', {
            url: '/cluster?clusterName',
            params: {
                clusterName: {
                    value: '',
                    squash: false
                }
            },
            templateUrl: 'views/index/shards.html',
            controller: 'ClusterCtl'
        })
        .state('cluster_dc', {
            url: '/cluster_dc?clusterName',
            templateUrl: 'views/index/cluster_dc.html',
            controller: 'ClusterDcCtl'
        })
        .state('cluster_list', {
            url: '/clusterlist',
            templateUrl: 'views/index/cluster_list.html',
            controller: 'ClusterListCtl'
        })
        .state('cluster_form', {
            url: '/cluster_form?clusterName',
            templateUrl: 'views/index/cluster_form.html',
            controller: 'ClusterFromCtl'

        });

});
