angular
    .module('services')
    .service('AppliercontainerService', ['$resource', '$q', function ($resource, $q) {

    var resource = $resource('', {}, {
        get_all_active_appliercontainer_infos: {
            method: 'GET',
            url: '/console/appliercontainer/infos/all',
            isArray: true
        },
        get_all_organizations: {
            method: 'GET',
            url: '/console/organizations',
            isArray: true
        },
        get_appliercontainer_by_id: {
            method: 'GET',
            url: '/console/appliercontainer/:appliercontainerId'
        },
        get_active_appliercontainers_by_dc_cluster: {
            method: 'GET',
            url: '/console/dcs/:dcName/clusters/:clusterName/active-appliercontainers',
            isArray: true
        },
        add_appliercontainer: {
            method: 'POST',
            url: '/console/appliercontainer'
        },
        update_appliercontainer: {
            method: 'PUT',
            url: '/console/appliercontainer'
        }
    });


    function getAllActiveAppliercontainerInfos() {
        var d = $q.defer();
        resource.get_all_active_appliercontainer_infos({},
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAppliercontainerById(appliercontainerId) {
        var d = $q.defer();
        resource.get_appliercontainer_by_id({
                appliercontainerId : appliercontainerId,
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getActiveAppliercontainersByDcCluster(dcName, clusterName) {
        var d = $q.defer();
        resource.get_active_appliercontainers_by_dc_cluster({
                dcName : dcName,
                clusterName : clusterName
            },
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

    function addAppliercontainer(addr, dcName, orgName, azName, active) {
        var d = $q.defer();
        resource.add_appliercontainer({}, {
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

    function updateAppliercontainer(addr, dcName, orgName, azName, active) {
        var d = $q.defer();
        resource.update_appliercontainer({}, {
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
        getAllOrganizations : getAllOrganizations,
        getAppliercontainerById : getAppliercontainerById,
        getActiveAppliercontainersByDcCluster : getActiveAppliercontainersByDcCluster,
        getAllActiveAppliercontainerInfos : getAllActiveAppliercontainerInfos,
        addAppliercontainer : addAppliercontainer,
        updateAppliercontainer : updateAppliercontainer,
    }
}]);
