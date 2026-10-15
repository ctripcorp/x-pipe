angular
    .module('services')
    .service('KeeperContainerService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        find_availablekeepers_by_dc: {
            method: 'POST',
            url: '/console/dcs/:dcName/availablekeepers',
            isArray : true
        },
        find_active_kcs_by_dc_and_cluster: {
            method: 'GET',
            url: '/console/dcs/:dcName/cluster/:clusterName/activekeepercontainers',
            isArray : true
        },
        find_keepercontainer_by_id: {
            method: 'GET',
            url: '/console/keepercontainer/:id',
        },
        find_available_keepers_by_dc_az_and_org: {
            method: 'GET',
            url: '/console/keepercontainers',
            params: {
                dcName: '@dcName',
                azName: '@azName',
                orgName: '@orgName',
                tag: '@tag'
            },
            isArray : true
        },
        get_all_infos: {
            method: 'GET',
            url: '/console/keepercontainer/infos/all',
            isArray : true
        },
        get_all_diskTypes: {
            method: 'GET',
            url: '/console/keepercontainer/diskType',
            isArray : true
        },
        get_all_organizations: {
            method: 'GET',
            url: '/console/organizations',
            isArray: true
        },
        add_keepercontainer:{
            method:'POST',
            url:'/console/keepercontainer'
        },
        update_keepercontainer:{
            method:'PUT',
            url:'/console/keepercontainer'
        },
        get_all_overload_keepercontainer: {
            method: 'GET',
            url: '/console/keepercontainers/overload/all',
            isArray: true
        },
        get_all_overload_lasted_used_info: {
            method: 'GET',
            url: '/console/keepercontainer/overload/info/lasted',
            isArray: true
        },
        get_all_keepers: {
            method: 'GET',
            url: '/console/keepercontainer/keepers/:keeperContainerId',
            isArray: true
        },
        get_overload_keepercontainer_migration_process: {
            method: 'GET',
            url: '/console/keepercontainer/overload/migration/process',
            isArray: true
        },
        get_all_available_zone_info_models_by_dc: {
            method: 'GET',
            url: '/console/az/dcs/tbl/:dcId',
            isArray: true
        },
        begin_to_migrate_overload_keepercontainer:{
            method:'POST',
            url:'/console/keepercontainer/overload/migration/begin'
        },
        migrate_keeper_task_terminate: {
            method:'POST',
            url:'/console/keepercontainer/overload/migration/terminate'
        },
        stop_to_migrate_overload_keepercontainer:{
            method:'POST',
            url:'/console/keepercontainer/overload/migration/stop'
        },
        reset_election:{
            method:'POST',
            url:'/api/keepers/election/reset'
        },
        release_rdb:{
            method:'POST',
            url:'/api/keepers/release/rdb'
        }
    });

    function findActiveKeeperContainersByDc(dcName) {
        var d = $q.defer();
        resource.find_activekeepercontainers_by_dc({
                            dcName: dcName
                        },
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findAvailableKeepersByDc(dcName, shard) {
        var d = $q.defer();
        resource.find_availablekeepers_by_dc({
                            dcName: dcName
                        },shard,
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findKeepercontainerById(id) {
        var d = $q.defer();
        resource.find_keepercontainer_by_id({
                            id: id
                        },
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findAvailableKeepersByDcAzOrgAndTag(dcName, azName, orgName, tag) {
        var d = $q.defer();
        resource.find_available_keepers_by_dc_az_and_org({
                            dcName: dcName,
                            azName: azName,
                            orgName: orgName,
                            tag: tag
                        },
                        function (result) {
                            d.resolve(result);
                        }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllAvailableZoneInfoModelsByDc(dcId) {
        var d = $q.defer();
        resource.get_all_available_zone_info_models_by_dc({
                dcId: dcId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findAvailableKeepersByDcAndCluster(dcName, clusterName) {
            var d = $q.defer();
            resource.find_active_kcs_by_dc_and_cluster({
                                dcName: dcName,
                                clusterName: clusterName
                            },
                            function (result) {
                                d.resolve(result);
                            }, function (result) {
                    d.reject(result);
                });
            return d.promise;
    }

    function getAllInfos() {
        var d = $q.defer();
        resource.get_all_infos({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllOrganizations() {
        var d = $q.defer();
        resource.get_all_organizations({},
                function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllDiskTypes() {
        var d = $q.defer();
        resource.get_all_diskTypes({},
            function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function addKeepercontainer(ip, keepercontainerHostName, port, dcName, orgName, azName, active, diskType, tag, logicalBuId) {
        var d = $q.defer();
        resource.add_keepercontainer({}, {
                    ip : ip,
                    keepercontainerHostName : keepercontainerHostName,
                    port : port,
                    dcName : dcName,
                    orgName : orgName,
                    azName : azName,
                    active :active,
                    diskType : diskType,
                    tag : tag,
                    logicalBuId : logicalBuId,
                },
                function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function updateKeepercontainer(ip, keepercontainerHostName, port, dcName, orgName, azName, active, diskType, tag, logicalBuId) {
        var d = $q.defer();
        resource.update_keepercontainer({}, {
                    ip : ip,
                    // hostname 是容器化 KC 的稳定标识，后端按它做双轨定位，必须回传
                    keepercontainerHostName : keepercontainerHostName,
                    port : port,
                    dcName : dcName,
                    orgName : orgName,
                    azName : azName,
                    active :active,
                    diskType : diskType,
                    tag : tag,
                    logicalBuId : logicalBuId
                },
                function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllOverloadKeepercontainer() {
        var d = $q.defer();
        resource.get_all_overload_keepercontainer({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllKeepercontainerUsedInfo() {
        var d = $q.defer();
        resource.get_all_overload_lasted_used_info({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllKeepers(keeperContainerId) {
        var d = $q.defer();
        resource.get_all_keepers({
                keeperContainerId:keeperContainerId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getOverloadKeeperContainerMigrationProcess() {
        var d = $q.defer();
        resource.get_overload_keepercontainer_migration_process({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function beginToMigrateOverloadKeeperContainers() {
        var d = $q.defer();
        resource.begin_to_migrate_overload_keepercontainer(
            Array.from(arguments),
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function migrateKeeperTaskTerminate() {
        var d = $q.defer();
        resource.migrate_keeper_task_terminate({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    // 后端是 @RequestBody KeeperRestElectionModel，参数必须放在请求体里（第二个参数），不能拼进 URL；
    // 容器 keeper 需要 hostname 才能定位，ip 可能已随 pod 重建变化
    function resetElection(dcName, ip, hostname, port, shardId) {
        var d = $q.defer();
        resource.reset_election({}, {
                dcName:dcName,
                ip:ip,
                hostname:hostname,
                port:port,
                shardId:shardId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function releaseRdb(dcName, ip, hostname, port, shardId) {
        var d = $q.defer();
        resource.release_rdb({}, {
                dcName:dcName,
                ip:ip,
                hostname:hostname,
                port:port,
                shardId:shardId
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        findAvailableKeepersByDc : findAvailableKeepersByDc,
        findAvailableKeepersByDcAndCluster : findAvailableKeepersByDcAndCluster,
        findKeepercontainerById : findKeepercontainerById,
        getAllAvailableZoneInfoModelsByDc : getAllAvailableZoneInfoModelsByDc,
        findAvailableKeepersByDcAzOrgAndTag : findAvailableKeepersByDcAzOrgAndTag,
        getAllInfos: getAllInfos,
        getAllOrganizations: getAllOrganizations,
        getAllDiskTypes: getAllDiskTypes,
        addKeepercontainer: addKeepercontainer,
        updateKeepercontainer: updateKeepercontainer,
        getAllOverloadKeepercontainer : getAllOverloadKeepercontainer,
        getAllKeepercontainerUsedInfo : getAllKeepercontainerUsedInfo,
        getAllKeepers:getAllKeepers,
        getOverloadKeeperContainerMigrationProcess : getOverloadKeeperContainerMigrationProcess,
        beginToMigrateOverloadKeeperContainers : beginToMigrateOverloadKeeperContainers,
        migrateKeeperTaskTerminate : migrateKeeperTaskTerminate,
        resetElection: resetElection,
        releaseRdb: releaseRdb
    }
}]);
