index_module.config(function ($stateProvider, $urlRouterProvider) {


    $urlRouterProvider.otherwise("/cluster_list");

    $stateProvider
        .state('cluster_shards', {
            url: '/cluster_shards?clusterName',
            templateUrl: 'views/index/cluster_shards.html',
            controller: 'ClusterShardCtl'
        })
        .state('cluster_dc_shards', {
            url: '/cluster_dc_shards/:clusterName/:currentDcName',
            params: {
                clusterName: {
                    value: '',
                    squash: false
                },
                currentDcName: {
                    value: '',
                    squash: false
                }
            },
            templateUrl: 'views/index/cluster_dc_shards.html',
            controller: 'ClusterCtl'
        })
        .state(
            'dc_list',{
                url: '/dc_list?dcName',
                templateUrl: 'views/index/dc_list.html',
                controller: 'DcListCtl'
            }
        )
        .state('cluster_dc_proxy_chains', {
            url: '/chain/:clusterName/:currentDcName',
            params: {
                clusterName: {
                    value: '',
                    squash: false
                },
                currentDcName: {
                    value: '',
                    squash: false
                }
            },
            templateUrl: 'views/index/cluster_proxy_chain.html',
            controller: 'ProxyChainCtl'
        })
        .state('proxy_tunnels', {
            url: '/proxy/:proxyIp/:dcId',
            params: {
                proxyIp: {
                    value: '',
                    squash: false
                },
                dcId: {
                    value: '',
                    squash: false
                }

            },
            templateUrl: 'views/index/proxy_tunnel.html',
            controller: 'TunnelsCtl'
        })
        .state('proxy_pings', {
            url: '/proxy/pings',
            params: {

            },
            templateUrl: 'views/index/proxy_ping.html',
            controller: 'ProxyPingCtl'
        })
        .state('proxy_overview', {
            url: '/proxy/overview',
            params: {
            },
            templateUrl: 'views/index/proxy_list.html',
            controller: 'ProxyListCtl'
        })
        .state('cluster_dc_shard_update', {
        	url: '/cluster_dc_shard_update?clusterName&shardName&currentDcName',
        	templateUrl: 'views/index/cluster_dc_shard_update.html',
        	controller: 'ClusterDcShardUpdateCtl'
        })
        .state('cluster_dc', {
            url: '/cluster_dc?clusterName',
            templateUrl: 'views/index/cluster_dc.html',
            controller: 'ClusterDcCtl'
        })
        .state('cluster_list', {
            url: '/cluster_list?clusterName&dcName&type',
            templateUrl: 'views/index/cluster_list.html',
            controller: 'ClusterListCtl'
        })
        .state('cluster_form', {
            url: '/cluster_form?clusterName&type',
            templateUrl: 'views/index/cluster_form.html',
            controller: 'ClusterFromCtl'

        })
        .state('migration_index', {
        	url: '/active_dc_migration',
        	templateUrl: 'views/index/migration_index.html',
        	controller: 'ActiveDcMigrationIndexCtl'
        })
        .state('migration_event_list', {
        	url: '/migration_event_list',
        	templateUrl: 'views/index/migration_list.html',
        	controller: 'ActiveDcMigrationEventListCtl'
        })
        .state('migration_event_details', {
        	url: '/migration_event_details/:eventId',
            params: {
                eventId: {
                    value: '',
                    squash: false
                }
            },
        	templateUrl: 'views/index/migration_details.html',
        	controller: 'ActiveDcMigrationEventDetailsCtl'
        })
        .state('migration_event_details.details', {
        	url: '/details',
            params: {
                migrationCluster: {
                    value: {},
                    squash: false
                }
            },
        	templateUrl: 'views/index/migration_details_content.html',
        	controller : 'ActiveDcMigrationEventDetailsContentCtl'
        });

});
