angular
    .module('index')
    .controller('RouteFormCtl', RouteFormCtl);

RouteFormCtl.$inject = ['$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'DcService', 'RouteService', 'ProxyService', 'ClusterType', 'NgTableParams'];

function RouteFormCtl($scope, $stateParams, $window, toastr, AppUtil, DcService, RouteService, ProxyService, ClusterType, NgTableParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});

    var OPERATE_TYPE = {
        CREATE: 'create',
        UPDATE: 'update',
    };

    $scope.tags = ['meta', 'console'];
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

    $scope.clusterTypes = ClusterType.selectData();

    $scope.doAddRoute = doAddRoute;

    init();

    function init() {
        DcService.loadAllDcs()
            .then(function (result) {
                $scope.allDcs = result;

                $scope.allDcs.forEach(function(dc){
                    ProxyService.getAllActiveProxyUrisByDc(dc.dcName)
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

        RouteService.getAllOrganizations()
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
                if ($scope.route.clusterType) {
                    let type = ClusterType.lookup($scope.route.clusterType);
                    if (type) $scope.route.clusterType = type.value
                }
            }, function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        } else {
            $scope.route = {};
            $scope.route.tag = $scope.tags[0];
            $scope.route.active = $scope.bools[0];
            $scope.route.public = $scope.bools[0];

        }
    }

    function doAddRoute() {
        if($scope.operateType == OPERATE_TYPE.CREATE) {
            RouteService.addRoute($scope.route.orgName, $scope.route.clusterType, $scope.route.srcProxies, $scope.route.optionalProxies, $scope.route.dstProxies,
                                        $scope.route.srcDcName, $scope.route.dstDcName, $scope.route.tag, $scope.route.active, $scope.route.public, $scope.route.description)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("添加成功");
                        $window.location.href = "/#/route/overview?srcDcName&dstDcName";
                    } else {
                        toastr.error(result.message, "添加失败");
                    }
                }, function(result) {
                    toastr.error(AppUtil.errorMsg(result), "添加失败");
                });
        } else {
            RouteService.updateRoute($scope.route.id, $scope.route.orgName, $scope.route.clusterType, $scope.route.srcProxies, $scope.route.optionalProxies, $scope.route.dstProxies,
                                        $scope.route.srcDcName, $scope.route.dstDcName, $scope.route.tag, $scope.route.active, $scope.route.public, $scope.route.description)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("修改成功");
                        $window.location.href = "/#/route/overview?srcDcName&dstDcName";
                    } else {
                        toastr.error(result.message, "修改失败");
                    }
                }, function(result) {
                   toastr.error(AppUtil.errorMsg(result), "更新失败");
                });
        }
    }

}
