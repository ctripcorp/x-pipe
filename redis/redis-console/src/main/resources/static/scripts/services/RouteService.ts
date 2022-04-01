angular
    .module('services')
    .service('RouteService', RouteService);

RouteService.$inject = ['$resource', '$q'];

function RouteService($resource, $q) {

    var resource = $resource('', {}, {
        get_active_routes_all:{
            method: 'GET',
            url: '/console/route/status/all',
            isArray: true
        },
        get_active_routes_by_tag:{
            method: 'GET',
            url: '/console/route/tag/:tag',
            isArray: true
        },
        get_route_by_tag_and_direction:{
            method: 'GET',
            url: 'console/route/tag/:tag/direction/:srcDcName/:dstDcName',
            isArray: true
        },
        get_route_by_id:{
            method: 'GET',
            url: '/console/route/id/:route_id',
        },
        get_route_direction_infos_by_tag:{
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

    function getAllActiveRoute() {
        var d = $q.defer();
        resource.get_active_routes_all({
            },
            function (result) {
                d.resolve(result);
            }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getOrganizations() {
        var d = $q.defer();
        resource.get_all_organizations({},
            function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });
        return d.promise;
    }

    function addRoute(orgName, srcProxies, optionalProxies, dstProxies, srcDcName, dstDcName, tag, active, public, description) {
        var d = $q.defer();
        resource.add_route({}, {
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

    function updateRoute(id, orgName, srcProxies, optionalProxies, dstProxies, srcDcName, dstDcName, tag, active, public, description) {
        var d = $q.defer();
        resource.update_route({}, {
            id: id,
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
        resource.update_routes({},{
                routeInfoModels : routes
            },function (result) {
                d.resolve(result);
            }, function (result) {
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

    function getAllActiveRouteRouteByTagAndDirection(tag, srcDcName, dstDcName) {
        var d = $q.defer();
        resource.get_route_by_tag_and_direction({
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

    function getAllActiveRouteRouteByTag(tag) {
        var d = $q.defer();
        resource.get_active_routes_by_tag({
            tag : tag
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }

    function getRoutesDirectionInfoByTag(tag) {
        var d = $q.defer();
        resource.get_route_direction_infos_by_tag({
            tag : tag
        }, function(result) {
            d.resolve(result);
        }, function(result) {
            d.reject(result);
        });

        return d.promise;
    }


    return {
        getAllActiveRoute : getAllActiveRoute,
        getOrganizations : getOrganizations,
        getRouteById : getRouteById,
        getAllActiveRouteRouteByTag : getAllActiveRouteRouteByTag,
        addRoute : addRoute,
        updateRoute : updateRoute,
        updateRoutes : updateRoutes,
        getRoutesDirectionInfoByTag : getRoutesDirectionInfoByTag,
        getAllActiveRouteRouteByTagAndDirection : getAllActiveRouteRouteByTagAndDirection
    }
}
