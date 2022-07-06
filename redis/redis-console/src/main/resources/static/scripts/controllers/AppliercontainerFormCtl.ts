angular
    .module('index')
    .controller('AppliercontainerFormCtl', AppliercontainerFormCtl);

AppliercontainerFormCtl.$inject = ['$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'AppliercontainerService', 'DcService', 'AzService', 'NgTableParams'];

function AppliercontainerFormCtl($scope, $stateParams, $window, toastr, AppUtil, AppliercontainerService, DcService, AzService, NgTableParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});

    var OPERATE_TYPE = {
        CREATE: 'create',
        UPDATE: 'update',
    };

    $scope.bools = [true, false];

    $scope.operateType = $stateParams.type;
    $scope.appliercontainerId = $stateParams.id;

    $scope.allDcs = [];
    $scope.dcNames = [];
    $scope.organizations = [];
    $scope.dcAzs = [];
    $scope.appliercontainer=[];

    $scope.doAddAppliercontainer = doAddAppliercontainer;

    init();

    function init() {
        DcService.loadAllDcs()
            .then(function (result) {
                $scope.allDcs = result;

                $scope.allDcs.forEach(function(dc){
                    AzService.getAllActiveAvailableZoneInfosByDc(dc.id)
                    .then(function(result) {
                        console.log(result);
                        $scope.dcAzs[dc.dcName] =  result.map(function (az) {
                            return az.azName;
                        });;
                    });
                });

                $scope.dcNames = result.map(function (dc) {
                    return dc.dcName;
                });
            });

        AppliercontainerService.getAllOrganizations()
            .then(function(result) {
                $scope.organizations = result;
                $scope.organizationNames = result.map(function (org) {
                    return org.orgName;
                });
            });

        if($scope.operateType != OPERATE_TYPE.CREATE) {
            console.log($scope.appliercontainerId);
            AppliercontainerService.getAppliercontainerById($scope.appliercontainerId)
            .then(function(result) {
                console.log(result);
                $scope.appliercontainer = result;
            }, function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        } else {
            $scope.appliercontainer = {};
        }
    }

    function doAddAppliercontainer() {
        if($scope.operateType == OPERATE_TYPE.CREATE) {
            AppliercontainerService.addAppliercontainer($scope.appliercontainer.addr, $scope.appliercontainer.dcName,
                        $scope.appliercontainer.orgName, $scope.appliercontainer.azName, $scope.appliercontainer.active)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("添加成功");
                        $window.location.href = "/#/appliercontainers";
                    } else {
                        toastr.error(result.message, "添加失败");
                    }
                }, function(result) {
                    toastr.error(AppUtil.errorMsg(result), "添加失败");
                });
        } else {
            AppliercontainerService.updateAppliercontainer($scope.appliercontainer.addr, $scope.appliercontainer.dcName,
                        $scope.appliercontainer.orgName, $scope.appliercontainer.azName, $scope.appliercontainer.active)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("修改成功");
                        $window.location.href = "/#/appliercontainers";
                    } else {
                        toastr.error(result.message, "修改失败");
                    }
                }, function(result) {
                   toastr.error(AppUtil.errorMsg(result), "修改失败");
                });
        }
    }
}
