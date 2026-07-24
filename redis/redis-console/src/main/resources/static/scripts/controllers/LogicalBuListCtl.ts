angular
    .module('index')
    .controller('LogicalBuListCtl', LogicalBuListCtl);

LogicalBuListCtl.$inject = ['$scope', 'toastr', 'AppUtil', 'LogicalBuService', 'ClusterService', 'NgTableParams'];

function LogicalBuListCtl($scope, toastr, AppUtil, LogicalBuService, ClusterService, NgTableParams) {

    $scope.originData = [];
    $scope.organizations = [];
    $scope.orgIdNameMap = {};
    $scope.editingBu = {};
    $scope.selectedOrgIds = [];
    $scope.isCreate = true;

    $scope.preCreate = preCreate;
    $scope.preEdit = preEdit;
    $scope.save = save;
    $scope.preDelete = preDelete;
    $scope.doDelete = doDelete;
    $scope.formatOrgIds = formatOrgIds;
    $scope.toggleOrg = toggleOrg;
    $scope.isOrgSelected = isOrgSelected;

    init();

    function init() {
        ClusterService.getOrganizations()
            .then(function (result) {
                $scope.organizations = result;
                result.forEach(function (org) {
                    $scope.orgIdNameMap[org.id] = org.orgName;
                });
            });
        loadAll();
    }

    function loadAll() {
        LogicalBuService.findAll()
            .then(function (result) {
                $scope.originData = result || [];
                $scope.tableParams = new NgTableParams({
                    page: 1,
                    count: 10
                }, {
                    filterDelay: 100,
                    counts: [10, 25, 50],
                    dataset: $scope.originData
                });
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result));
            });
    }

    function preCreate() {
        $scope.isCreate = true;
        $scope.editingBu = {
            name: '',
            tfsFsId: '',
            active: true,
            description: '',
            cmsOrgIds: []
        };
        $scope.selectedOrgIds = [];
        $('#logicalBuModal').modal('show');
    }

    function preEdit(bu) {
        $scope.isCreate = false;
        $scope.editingBu = {
            id: bu.id,
            name: bu.name,
            tfsFsId: bu.tfsFsId,
            active: bu.active,
            description: bu.description || '',
            cmsOrgIds: bu.cmsOrgIds || [],
            keeperContainerCount: bu.keeperContainerCount || 0
        };
        $scope.selectedOrgIds = (bu.cmsOrgIds || []).slice();
        $('#logicalBuModal').modal('show');
    }

    function toggleOrg(orgId) {
        var idx = $scope.selectedOrgIds.indexOf(orgId);
        if (idx > -1) {
            $scope.selectedOrgIds.splice(idx, 1);
        } else {
            $scope.selectedOrgIds.push(orgId);
        }
    }

    function isOrgSelected(orgId) {
        return $scope.selectedOrgIds.indexOf(orgId) > -1;
    }

    function save() {
        $scope.editingBu.cmsOrgIds = $scope.selectedOrgIds.slice();
        if ($scope.isCreate) {
            LogicalBuService.create($scope.editingBu)
                .then(function () {
                    toastr.success('创建成功');
                    $('#logicalBuModal').modal('hide');
                    loadAll();
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), '创建失败');
                });
        } else {
            LogicalBuService.update($scope.editingBu.id, $scope.editingBu)
                .then(function () {
                    toastr.success('更新成功');
                    $('#logicalBuModal').modal('hide');
                    loadAll();
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result), '更新失败');
                });
        }
    }

    function preDelete(bu) {
        $scope.toDeleteBu = bu;
        $('#deleteLogicalBuConfirm').modal('show');
    }

    function doDelete() {
        LogicalBuService.remove($scope.toDeleteBu.id)
            .then(function () {
                toastr.success('删除成功');
                $('#deleteLogicalBuConfirm').modal('hide');
                loadAll();
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '删除失败');
            });
    }

    function formatOrgIds(cmsOrgIds) {
        if (!cmsOrgIds || !cmsOrgIds.length) {
            return '-';
        }
        return cmsOrgIds.map(function (id) {
            return $scope.orgIdNameMap[id] || id;
        }).join(', ');
    }
}
