angular
    .module('index')
    .controller('RouteFormCtl', RouteFormCtl);

RouteFormCtl.$inject = ['$rootScope', '$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'DcService', 'RouteService', 'ProxyService', 'NgTableParams'];

function RouteFormCtl($rootScope, $scope, $stateParams, $window, toastr, AppUtil, DcService, RouteService, ProxyService, NgTableParams) {

    $rootScope.currentNav = '1-3';
    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});

    var OPERATE_TYPE = {
        CREATE: 'create',
        UPDATE: 'update',
        RETRIEVE: 'retrieve'
    };

    $scope.tags = ['META', 'CONSOLE'];
    $scope.bools = [true, false];

    $scope.operateType = $stateParams.type;
    $scope.routeId = $stateParams.routeId;
    $scope.allDcs = [];
    $scope.dcNames = [];
    $scope.organizations = [];
    $scope.organizationNames = [];
    $scope.allProxies = [];
    $scope.dcProxies = [];
    $scope.selectSrcProxies = [];
    $scope.selectOptionalProxies = [];
    $scope.selectDstProxies = [];

    $scope.doAddRoute = doAddRoute;

    init();

    function init() {
        DcService.loadAllDcs()
            .then(function (result) {
                $scope.allDcs = result;

                $scope.allDcs.forEach(function(dc){
                    ProxyService.getAllProxyUriByDc(dc.dcName)
                    .then(function(result) {
                        $scope.dcProxies[dc.dcName] = result;
                        result.forEach(function(proxy) {
                            $scope.allProxies.push(proxy);
                        });
                    });
                });

                $scope.dcNames = result.map(function (dc) {
                    return dc.dcName;
                });
            });

        RouteService.getOrganizations()
            .then(function(result) {
                $scope.organizations = result;
                $scope.organizationNames = result.map(function (org) {
                    return org.orgName;
                });
            });

        if($scope.operateType != OPERATE_TYPE.CREATE) {
            RouteService.getRouteById($scope.routeId)
            .then(function(result) {
                $scope.route = result;
            }, function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        } else {
            $scope.route = {};
            $window.route = $scope.route;
        }
    }

    function doAddRoute() {
        console.log("1111");
        if($scope.operateType == OPERATE_TYPE.CREATE) {
            RouteService.addRoute($scope.route.orgName, $scope.route.srcProxies, $scope.route.optionalProxies, $scope.route.dstProxies,
                                        $scope.route.srcDcName, $scope.route.dstDcName, $scope.route.tag, $scope.route.active, $scope.route.description)
                .then(function(result) {
                    toastr.success("添加成功");
                    $window.location.href = "/#/route/overview";
                }, function(result) {
                    toastr.error(AppUtil.errorMsg(result), "添加失败");
                });
        } else {
            console.log($scope.route);
            RouteService.updateRoute($scope.route.id, $scope.route.orgName, $scope.route.srcProxies, $scope.route.optionalProxies, $scope.route.dstProxies,
                                        $scope.route.srcDcName, $scope.route.dstDcName, $scope.route.tag, $scope.route.active, $scope.route.description)
                .then(function(result) {
                    toastr.success("更新成功");
                    $window.location.href =
                        "/#/route_form?routeId=" + result.id + "&type=retrieve";
                }, function(result) {
                   toastr.error(AppUtil.errorMsg(result), "更新失败");
                });
        }
    }

}