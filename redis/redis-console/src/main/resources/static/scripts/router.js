index_module.config(function($stateProvider, $urlRouterProvider) {

    $stateProvider
        .state('cluster', {
            url: '/cluster',
            templateUrl: 'views/index/cluster.html',
            controller: 'ClusterCtl'
        })
        .state('page2', {
            url: 'page2',
            templateUrl: 'views/index/page2.html'
        });

});
