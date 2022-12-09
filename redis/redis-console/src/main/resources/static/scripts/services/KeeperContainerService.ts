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
            url: '/console/keepercontainers/dc/:dcName/az/:azName/org/:orgName',
            isArray : true
        },
        get_all_infos: {
            method: 'GET',
            url: '/console/keepercontainer/infos/all',
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

    function findAvailableKeepersByDcAzAndOrg(dcName, azName, orgName) {
        var d = $q.defer();
        resource.find_available_keepers_by_dc_az_and_org({
                            dcName: dcName,
                            azName: azName,
                            orgName: orgName
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

    function addKeepercontainer(addr, dcName, orgName, azName, active) {
        var d = $q.defer();
        resource.add_keepercontainer({}, {
                    addr : addr,
                    dcName : dcName,
                    orgName : orgName,
                    azName : azName,
                    active :active
                },
                function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function updateKeepercontainer(addr, dcName, orgName, azName, active) {
        var d = $q.defer();
        resource.update_keepercontainer({}, {
                    addr : addr,
                    dcName : dcName,
                    orgName : orgName,
                    azName : azName,
                    active :active
                },
                function(result) {
                d.resolve(result);
            }, function(result) {
                d.reject(result);
            });
        return d.promise;
    }


    return {
        findAvailableKeepersByDc : findAvailableKeepersByDc,
        findAvailableKeepersByDcAndCluster : findAvailableKeepersByDcAndCluster,
        findKeepercontainerById : findKeepercontainerById,
        findAvailableKeepersByDcAzAndOrg : findAvailableKeepersByDcAzAndOrg,
        getAllInfos: getAllInfos,
        getAllOrganizations: getAllOrganizations,
        addKeepercontainer: addKeepercontainer,
        updateKeepercontainer: updateKeepercontainer,
    }
}]);
