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
        .state('page2', {
            url: 'page2',
            templateUrl: 'views/index/page2.html'
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
