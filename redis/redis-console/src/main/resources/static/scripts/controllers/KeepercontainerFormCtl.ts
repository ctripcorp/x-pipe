angular
    .module('index')
    .controller('KeepercontainerFormCtl', KeepercontainerFormCtl);

KeepercontainerFormCtl.$inject = ['$scope', '$stateParams', '$window', 'toastr', 'AppUtil', 'KeeperContainerService', 'DcService', 'AzService', 'NgTableParams'];

function KeepercontainerFormCtl($scope, $stateParams, $window, toastr, AppUtil, KeeperContainerService, DcService, AzService, NgTableParams) {

    $scope.routes = {};
    $scope.tableParams = new NgTableParams({}, {});

    var OPERATE_TYPE = {
        CREATE: 'create',
        UPDATE: 'update',
    };

    $scope.bools = [true, false];

    $scope.operateType = $stateParams.type;
    $scope.keepercontainerId = $stateParams.id;

    $scope.allDcs = [];
    $scope.dcNames = [];
    $scope.organizations = [];
    $scope.dcAzs = [];
    $scope.keepercontainer=[];

    $scope.doAddKeepercontainer = doAddKeepercontainer;

    init();

    function init() {
        DcService.loadAllDcs()
            .then(function (result) {
                $scope.allDcs = result;

                $scope.allDcs.forEach(function(dc){
                    AzService.getAllActiveAvailableZoneInfosByDc(dc.id)
                    .then(function(result) {
                        $scope.dcAzs[dc.dcName] =  result.map(function (az) {
                            return az.azName;
                        });;
                    });
                });

                $scope.dcNames = result.map(function (dc) {
                    return dc.dcName;
                });
            });

        KeeperContainerService.getAllOrganizations()
            .then(function(result) {
                $scope.organizations = result;
                $scope.organizationNames = result.map(function (org) {
                    return org.orgName;
                });
            });

        KeeperContainerService.getAllDiskTypes()
            .then(function(result) {
                $scope.diskTypes = result;
            })

        if($scope.operateType != OPERATE_TYPE.CREATE) {
            KeeperContainerService.findKeepercontainerById($scope.keepercontainerId)
            .then(function(result) {
                $scope.keepercontainer = result;
            }, function(result) {
                toastr.error(AppUtil.errorMsg(result));
            });
        } else {
            $scope.keepercontainer = {};
        }
    }

    function doAddKeepercontainer() {
        if($scope.operateType == OPERATE_TYPE.CREATE) {
            KeeperContainerService.addKeepercontainer($scope.keepercontainer.addr, $scope.keepercontainer.dcName,
                        $scope.keepercontainer.orgName, $scope.keepercontainer.azName, $scope.keepercontainer.active, $scope.keepercontainer.diskType)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("添加成功");
                        $window.location.href = "/#/keepercontainers";
                    } else {
                        toastr.error(result.message, "添加失败");
                    }
                }, function(result) {
                    toastr.error(AppUtil.errorMsg(result), "添加失败");
                });
        } else {
            KeeperContainerService.updateKeepercontainer($scope.keepercontainer.addr, $scope.keepercontainer.dcName,
                        $scope.keepercontainer.orgName, $scope.keepercontainer.azName, $scope.keepercontainer.active, $scope.keepercontainer.diskType)
                .then(function(result) {
                    if(result.message == 'success' ) {
                        toastr.success("修改成功");
                        $window.location.href = "/#/keepercontainers";
                    } else {
                        toastr.error(result.message, "修改失败");
                    }
                }, function(result) {
                   toastr.error(AppUtil.errorMsg(result), "修改失败");
                });
        }
    }
}
