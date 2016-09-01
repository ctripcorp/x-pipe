index_module.config(function ($stateProvider, $urlRouterProvider) {

    $stateProvider
        .state('cluster_shards', {
            url: '/cluster_shards?clusterName',
            templateUrl: 'views/index/cluster_shards.html',
            controller: 'ClusterShardCtl'
        })
        .state('cluster_dc_shards', {
            url: '/cluster_dc_shards?clusterName',
            params: {
                clusterName: {
                    value: '',
                    squash: false
                }
            },
            templateUrl: 'views/index/cluster_dc_shards.html',
            controller: 'ClusterCtl'
        })
        .state('cluster_dc_shard_update', {
        	url: '/cluster_dc_shard_update?clusterName&shardName',
        	templateUrl: 'views/index/cluster_dc_shard_update.html',
        	controller: 'ClusterDcShardUpdateCtl'
        })
        .state('cluster_dc', {
            url: '/cluster_dc?clusterName',
            templateUrl: 'views/index/cluster_dc.html',
            controller: 'ClusterDcCtl'
        })
        .state('cluster_list', {
            url: '/cluster_list?clusterName',
            templateUrl: 'views/index/cluster_list.html',
            controller: 'ClusterListCtl'
        })
        .state('cluster_form', {
            url: '/cluster_form?clusterName&type',
            templateUrl: 'views/index/cluster_form.html',
            controller: 'ClusterFromCtl'

        });

});
