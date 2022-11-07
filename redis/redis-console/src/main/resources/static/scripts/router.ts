angular
    .module('index')
    .config(router)
    .config(['$locationProvider', function($locationProvider) {
        $locationProvider.hashPrefix('');
    }]);

function router($stateProvider, $urlRouterProvider) {

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
        .state('route_overview', {
            url: '/route/overview?srcDcName&dstDcName',
            params: {
            },
            templateUrl: 'views/index/route_list.html',
            controller: 'RouteListCtl'
        })
        .state('route_form',{
            url: '/route_form?routeId&type',
            templateUrl: 'views/index/route_form.html',
            controller: 'RouteFormCtl'
        })
        .state('route_direction',{
            url: '/route_direction/route',
            templateUrl: 'views/index/route_direction.html',
            controller: 'RouteDirectionCtl'
        })
        .state('route_switch',{
            url: '/route_switch?tag&srcDcName&dstDcName',
            templateUrl: 'views/index/route_switch.html',
            controller: 'RouteSwitchCtl'
        })
        .state('cluster_routes', {
            url: '/cluster_routes?clusterName&dcName',
            templateUrl: 'views/index/cluster_routes.html',
            controller: 'ClusterRoutesCtl'
        })
        .state('cluster_designated_routes_update', {
        	url: '/cluster_designated_routes_update?clusterName&srcDcName',
        	templateUrl: 'views/index/cluster_designated_routes_update.html',
        	controller: 'ClusterDesignatedRoutesUpdateCtl'
        })
        .state('cluster_dc_shard_update', {
        	url: '/cluster_dc_shard_update?clusterName&shardName&currentDcName&srcDcName',
        	templateUrl: 'views/index/cluster_dc_shard_update.html',
        	controller: 'ClusterDcShardUpdateCtl'
        })
        .state('cluster_dc', {
            url: '/cluster_dc?clusterName',
            templateUrl: 'views/index/cluster_dc.html',
            controller: 'ClusterDcCtl'
        })
        .state('cluster_list', {
            url: '/cluster_list?clusterName&dcName&type&clusterType?keepercontainer',
            templateUrl: 'views/index/cluster_list.html',
            controller: 'ClusterListCtl'
        })
        .state('shard_list', {
            url: '/shard_list',
            templateUrl: 'views/index/shard_list.html',
            controller: 'ShardListCtl'
        })
        .state('cluster_form', {
            url: '/cluster_form?clusterName&type',
            templateUrl: 'views/index/cluster_form.html',
            controller: 'ClusterFromCtl'

        })
        .state('migration_index', {
        	url: '/active_dc_migration?:clusterName',
            params: {
                clusters: {
                    value: [],
                    squash: false
                }
            },
            templateUrl: 'views/index/migration_index.html',
        	controller: 'ActiveDcMigrationIndexCtl'
        })
        .state('migration_event_list', {
        	url: '/migration_event_list?clusterName',
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
        })
        .state('repl_direction_list', {
            url: '/repl_directions',
            templateUrl: 'views/index/repl_direction_list.html',
            controller : 'ReplDirectionListCtl',
        })
        .state('keepercontainer_list', {
            url: '/keepercontainers',
            templateUrl: 'views/index/keepercontainer_list.html',
            controller : 'KeeperContainerListCtl',
        })
        .state('keeper_migration', {
            url: '/keeper_migration?keepercontainer',
            templateUrl: 'views/index/keeper_migration.html',
            controller: 'KeeperMigrationCtl'
        })
        .state('appliercontainer_list', {
            url: '/appliercontainers',
            templateUrl: 'views/index/appliercontainer_list.html',
            controller : 'AppliercontainerListCtl',
        })
        .state('appliercontainer_form',{
            url: '/appliercontainer_form?id&type',
            templateUrl: 'views/index/appliercontainer_form.html',
            controller: 'AppliercontainerFormCtl'
        });

}
