angular
    .module('services')
    .service('RouteService', RouteService);

RouteService.$inject = ['$resource', '$q'];

function RouteService($resource, $q) {

    var resource = $resource('', {}, {
        get_all_active_routes:{
            method: 'GET',
            url: '/console/route/status/all',
            isArray: true
        },
        get_all_active_routes_by_tag:{
            method: 'GET',
            url: '/console/route/tag/:tag',
            isArray: true
        },
        get_route_by_tag_and_src_dc_name:{
            method: 'GET',
            url: 'console/route/src-dc/:srcDcName/',
            isArray: true
        },
        get_all_active_routes_by_tag_and_direction:{
            method: 'GET',
            url: 'console/route/tag/:tag/direction/:srcDcName/:dstDcName',
            isArray: true
        },
        get_route_by_id:{
            method: 'GET',
            url: '/console/route/id/:route_id',
        },
        get_all_route_direction_infos_by_tag:{
            method: 'GET',
            url: '/console/route/direction/tag/:tag',
            isArray: true
        },
        get_all_organizations: {
            method: 'GET',
            url: '/console/organizations',
            isArray : true
        },
        add_route: {
            method: 'POST',
            url: '/console/route'
        },
        update_route: {
            method: 'PUT',
            url: '/console/route'
        },
        update_routes: {
            method: 'PUT',
            url: '/console/routes'
        }
    });

    function getAllActiveRoutes() {
        var d = $q.defer();
        resource.get_all_active_routes({
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getAllActiveRoutesByTag(tag) {
        var d = $q.defer();
        resource.get_all_active_routes_by_tag({
            tag : tag
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function getAllActiveRoutesBySrcDcName(srcDcName) {
        var d = $q.defer();
        resource.get_route_by_tag_and_src_dc_name({
            srcDcName : srcDcName
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function getAllActiveRoutesByTagAndDirection(tag, srcDcName, dstDcName) {
        var d = $q.defer();
        resource.get_all_active_routes_by_tag_and_direction({
            tag : tag,
            srcDcName : srcDcName,
            dstDcName : dstDcName
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function getRouteById(route_id) {
        var d = $q.defer();
        resource.get_route_by_id({
            route_id : route_id
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function getAllRouteDirectionInfosByTag(tag) {
        var d = $q.defer();
        resource.get_all_route_direction_infos_by_tag({
            tag : tag
        }, function(result) {
            d.resolve(result);
        }, function(result) {
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

    function addRoute(orgName, clusterType, srcProxies, optionalProxies, dstProxies, srcDcName, dstDcName, tag, active, public, description) {
        var d = $q.defer();
        resource.add_route({}, {
            clusterType: clusterType,
            orgName: orgName,
            srcProxies : srcProxies,
            optionalProxies : optionalProxies,
            dstProxies : dstProxies,
            srcDcName : srcDcName,
            dstDcName : dstDcName,
            tag : tag,
            active : active,
            public : public,
            description : description
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function updateRoute(id, orgName, clusterType, srcProxies, optionalProxies, dstProxies, srcDcName, dstDcName, tag, active, public, description) {
        var d = $q.defer();
        resource.update_route({}, {
            id: id,
            clusterType: clusterType,
            orgName: orgName,
            srcProxies : srcProxies,
            optionalProxies : optionalProxies,
            dstProxies : dstProxies,
            srcDcName : srcDcName,
            dstDcName : dstDcName,
            tag : tag,
            active : active,
            public : public,
            description : description
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function updateRoutes(routes) {
        var d = $q.defer();
        resource.update_routes(
                Array.from(arguments)
            ,function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    return {
        getAllActiveRoutes : getAllActiveRoutes,
        getAllActiveRoutesByTag : getAllActiveRoutesByTag,
        getAllActiveRoutesBySrcDcName : getAllActiveRoutesBySrcDcName,
        getAllActiveRoutesByTagAndDirection : getAllActiveRoutesByTagAndDirection,
        getRouteById : getRouteById,
        getAllRouteDirectionInfosByTag : getAllRouteDirectionInfosByTag,
        getAllOrganizations : getAllOrganizations,
        addRoute : addRoute,
        updateRoute : updateRoute,
        updateRoutes : updateRoutes
    }
}
