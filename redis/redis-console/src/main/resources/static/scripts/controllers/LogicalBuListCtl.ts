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
    $scope.allLogicalBuData = [];
    $scope.logicalBuNameFilterData = [];
    $scope.idcFilterData = [];
    $scope.multiFilterSearch = {};
    $scope.allLogicalBusSelected = false;
    $scope.isCreate = true;
    $scope.showKeeperCount = false;
    $scope.refreshingKeeperState = false;
    // logicalBuId -> BuState from /keeperState; survives table reloads
    $scope.keeperStateByBu = {};
    $scope.baseLogicalBus = [];

    $scope.preCreate = preCreate;
    $scope.preEdit = preEdit;
    $scope.save = save;
    $scope.preDelete = preDelete;
    $scope.doDelete = doDelete;
    $scope.formatOrgIds = formatOrgIds;
    $scope.toggleOrg = toggleOrg;
    $scope.isOrgSelected = isOrgSelected;
    $scope.refreshKeeperState = refreshKeeperState;
    $scope.preSwitchBm = preSwitchBm;
    $scope.preRecoverTfs = preRecoverTfs;
    $scope.preSwitchTfs = preSwitchTfs;
    $scope.runSwitchBm = runSwitchBm;
    $scope.runRecoverTfs = runRecoverTfs;
    $scope.runSwitchTfs = runSwitchTfs;
    $scope.toggleAllLogicalBus = toggleAllLogicalBus;
    $scope.selectedLogicalBuCount = selectedLogicalBuCount;
    $scope.selectedLogicalBuNames = selectedLogicalBuNames;
    $scope.selectedLogicalBuIdcs = selectedLogicalBuIdcs;
    $scope.formatSelectedIdcs = formatSelectedIdcs;
    $scope.clearLogicalBuFilters = clearLogicalBuFilters;
    $scope.toggleTableMultiFilter = toggleTableMultiFilter;
    $scope.isTableMultiFilterSelected = isTableMultiFilterSelected;
    $scope.tableMultiFilterLabel = tableMultiFilterLabel;
    $scope.clearTableMultiFilter = clearTableMultiFilter;

    init();

    // Esc closes the innermost open detail: modal first, then a column filter dropdown, then Unknown lists.
    // Capture phase: runs before Bootstrap's own handlers, so one Esc closes exactly one layer.
    function onEscKey(event) {
        if (event.key !== 'Escape' && event.keyCode !== 27) {
            return;
        }
        var openModals = $('.modal.in');
        if (openModals.length) {
            openModals.last().modal('hide');
            return;
        }
        var openDropdowns = $('.logical-bu-multi-filter.open');
        if (openDropdowns.length) {
            openDropdowns.removeClass('open').find('.dropdown-toggle').attr('aria-expanded', 'false');
            return;
        }
        var expanded = ($scope.originData || []).filter(function (bu) {
            return bu.showUnknown;
        });
        if (expanded.length) {
            $scope.$apply(function () {
                expanded.forEach(function (bu) {
                    bu.showUnknown = false;
                });
            });
        }
    }
    document.addEventListener('keydown', onEscKey, true);
    $scope.$on('$destroy', function () {
        document.removeEventListener('keydown', onEscKey, true);
    });
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
                setTableData(result || []);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '加载逻辑BU失败');
                setTableData([]);
            });
    }

    function setTableData(data, keepSelectedIds?) {
        $scope.baseLogicalBus = data;
        applyKeeperStateIdcs(data);
        var expandedData = expandLogicalBuRows(data);
        expandedData.forEach(function (bu) {
            bu.selected = !!(keepSelectedIds && keepSelectedIds.indexOf(bu.id) > -1);
        });
        $scope.allLogicalBuData = expandedData;
        $scope.logicalBuNameFilterData.length = 0;
        expandedData.forEach(function (bu) {
            var exists = $scope.logicalBuNameFilterData.some(function (option) {
                return option.id === bu.name;
            });
            if (!exists) {
                $scope.logicalBuNameFilterData.push({id: bu.name, title: bu.name});
            }
        });
        $scope.idcFilterData.length = 0;
        var idcNames = [];
        expandedData.forEach(function (bu) {
            if (bu.idcName && idcNames.indexOf(bu.idcName) < 0) {
                idcNames.push(bu.idcName);
            }
        });
        idcNames.forEach(function (idcName) {
            $scope.idcFilterData.push({id: idcName, title: idcName});
        });
        $scope.allLogicalBusSelected = false;
        renderTableData(expandedData);
    }

    function expandLogicalBuRows(data) {
        var result = [];
        data.forEach(function (bu) {
            var idcNames = bu.idcNames && bu.idcNames.length ? bu.idcNames : [''];
            idcNames.forEach(function (idcName) {
                var row = angular.copy(bu);
                row.idcName = idcName;
                row.rowKey = bu.id + '@' + idcName;
                // shown and filtered as text: NULL is "-"
                row.priorityText = bu.priority == null ? '-' : String(bu.priority);
                // containers of this BU in this IDC; the edit modal keeps the BU total (keeperContainerCount)
                row.idcKeeperContainerCount = idcName
                    ? ((bu.idcKeeperContainerCounts || {})[idcName] || 0)
                    : (bu.keeperContainerCount || 0);
                attachKeeperState(row);
                result.push(row);
            });
        });
        return result;
    }

    function renderTableData(data) {
        $scope.originData = data;
        $scope.tableParams = new NgTableParams({
            page: 1,
            count: 10
        }, {
            filterDelay: 100,
            filterOptions: {
                filterDelay: 100,
                filterFn: tableFilter
            },
            counts: [10, 25, 50],
            dataset: $scope.originData
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

    /** TFS keepercontainer states of every logical BU in the table, per IDC (HEALTHY / DOWN / PENDING / UNKNOWN). */
    function refreshKeeperState() {
        var ids = [];
        $scope.baseLogicalBus.forEach(function (bu) {
            if (bu.id && ids.indexOf(bu.id) < 0) {
                ids.push(bu.id);
            }
        });
        if (!ids.length || $scope.refreshingKeeperState) {
            return;
        }
        var selectedIds = selectedLogicalBuIds();
        $scope.refreshingKeeperState = true;
        LogicalBuService.keeperState(ids)
            .then(function (result) {
                var byBu = {};
                (result.results || []).forEach(function (bu) {
                    byBu[bu.logicalBuId] = bu;
                });
                $scope.keeperStateByBu = byBu;
                $scope.showKeeperCount = true;
                setTableData($scope.baseLogicalBus, selectedIds);
            }, function (result) {
                toastr.error(AppUtil.errorMsg(result), '刷新容器状态失败');
            })
            .finally(function () {
                $scope.refreshingKeeperState = false;
            });
    }

    /** IDCs come from the BU's keepercontainers (backend); add any TFS IDC reported by /keeperState that is missing. */
    function applyKeeperStateIdcs(data) {
        data.forEach(function (bu) {
            var state = $scope.keeperStateByBu[bu.id];
            if (state && state.idcs && state.idcs.length) {
                var idcNames = (bu.idcNames || []).slice();
                state.idcs.forEach(function (idc) {
                    if (idc.idc && idcNames.indexOf(idc.idc) < 0) {
                        idcNames.push(idc.idc);
                    }
                });
                bu.idcNames = idcNames;
            }
        });
    }

    function attachKeeperState(row) {
        var state = $scope.keeperStateByBu[row.id];
        row.keeperStateError = state && state.failure
            ? (state.failure.message || state.failure.code) + (state.failure.detail ? '：' + state.failure.detail : '')
            : null;
        var idc = null;
        if (state && state.idcs) {
            state.idcs.forEach(function (item) {
                if (item.idc === row.idcName) {
                    idc = item;
                }
            });
        }
        row.keeperState = idc || {healthy: 0, down: 0, pending: 0, unknown: 0, containers: []};
        row.unknownContainers = (row.keeperState.containers || []).filter(function (container) {
            return container.state === 'UNKNOWN';
        });
    }
    function preSwitchBm() {
        $scope.switchBmResult = newSwitchResult();
        $('#switchBmModal').modal('show');
    }

    function newSwitchResult() {
        var selected = selectedFilteredLogicalBus();
        return {
            completed: false,
            running: false,
            // snapshot: the table selection is reset by loadAll() after execution
            logicalBuIds: selectedLogicalBuIds(),
            logicalBuNames: uniqueValues(selected, 'name'),
            idcs: uniqueValues(selected, 'idcName'),
            successCount: 0,
            failedCount: 0,
            failureReasons: [],
            failureDetails: []
        };
    }

    function uniqueValues(rows, key) {
        var values = [];
        rows.forEach(function (row) {
            var v = row[key];
            if (v != null && v !== '' && values.indexOf(v) < 0) {
                values.push(v);
            }
        });
        return values;
    }

    /** Server expands a BU to all its IDCs; show that instead of the rows the user happened to tick. */
    function applyServerScope(target, result) {
        var names = uniqueValues(result.results || [], 'logicalBuName');
        if (names.length) {
            target.logicalBuNames = names;
        }
        if (result.idcs && result.idcs.length) {
            target.idcs = result.idcs;
        }
    }

    function preRecoverTfs() {
        $scope.recoverTfsResult = newSwitchResult();
        $('#recoverTfsModal').modal('show');
    }
    function preSwitchTfs() {
        $scope.switchTfsResult = newSwitchResult();
        $('#switchTfsModal').modal('show');
    }

    function currentFilteredLogicalBus() {
        var currentFilter = $scope.tableParams ? $scope.tableParams.filter() : {};
        return tableFilter($scope.allLogicalBuData, currentFilter);
    }

    function selectedFilteredLogicalBus() {
        return currentFilteredLogicalBus().filter(function (bu) {
            return bu.selected;
        });
    }

    function toggleAllLogicalBus() {
        currentFilteredLogicalBus().forEach(function (bu) {
            bu.selected = $scope.allLogicalBusSelected;
        });
    }

    function refreshAllSelectionState(filter) {
        var rows = tableFilter($scope.allLogicalBuData, filter || {});
        $scope.allLogicalBusSelected = rows.length > 0 && rows.every(function (bu) {
            return bu.selected;
        });
    }

    function selectedLogicalBuCount() {
        return selectedFilteredLogicalBus().length;
    }

    function selectedLogicalBuNames() {
        var names = [];
        selectedFilteredLogicalBus().forEach(function (bu) {
            if (names.indexOf(bu.name) < 0) {
                names.push(bu.name);
            }
        });
        return names.length ? names.join('、') : '-';
    }

    function selectedLogicalBuIdcs() {
        var idcs = [];
        selectedFilteredLogicalBus().forEach(function (bu) {
            if (bu.idcName && idcs.indexOf(bu.idcName) < 0) {
                idcs.push(bu.idcName);
            }
        });
        return idcs;
    }

    function toggleTableMultiFilter(params, name, value, event) {
        if (event) {
            event.stopPropagation();
        }
        var nextFilter = angular.copy(params.filter());
        var values = (nextFilter[name] || []).slice();
        var idx = values.indexOf(value);
        if (idx > -1) {
            values.splice(idx, 1);
        } else {
            values.push(value);
        }
        if (values.length) {
            nextFilter[name] = values;
        } else {
            delete nextFilter[name];
        }
        params.filter(nextFilter);
        refreshAllSelectionState(nextFilter);
    }

    function isTableMultiFilterSelected(params, name, value) {
        var values = params.filter()[name] || [];
        return values.indexOf(value) > -1;
    }

    function tableMultiFilterLabel(params, name) {
        var values = params.filter()[name] || [];
        if (!values.length) {
            return '';
        }
        return values.join(', ');
    }

    function clearTableMultiFilter(params, name, event) {
        if (event) {
            event.stopPropagation();
        }
        var nextFilter = angular.copy(params.filter());
        delete nextFilter[name];
        $scope.multiFilterSearch[name] = '';
        params.filter(nextFilter);
        refreshAllSelectionState(nextFilter);
    }

    function clearLogicalBuFilters() {
        $scope.multiFilterSearch = {};
        $scope.allLogicalBusSelected = false;
        $scope.allLogicalBuData.forEach(function (bu) {
            bu.selected = false;
        });
        if ($scope.tableParams) {
            $scope.tableParams.filter({});
            $scope.tableParams.page(1);
            $scope.tableParams.reload();
        }
    }

    function tableFilter(data, filter) {
        var filterKeys = Object.keys(filter || {});
        if (!filterKeys.length) {
            return data;
        }
        return data.filter(function (bu) {
            return filterKeys.every(function (key) {
                var expected = filter[key];
                if (expected == null || expected === '' || (Array.isArray(expected) && !expected.length)) {
                    return true;
                }
                if (Array.isArray(expected)) {
                    return expected.indexOf(bu[key]) > -1;
                }
                var actual = bu[key];
                return actual != null && String(actual).toLowerCase().indexOf(String(expected).toLowerCase()) > -1;
            });
        });
    }

    function formatSelectedIdcs(idcNames) {
        return idcNames && idcNames.length ? idcNames.join('、') : '-';
    }

    function runSwitchBm() {
        runSwitch($scope.switchBmResult, LogicalBuService.switchBM, '强切 BM 请求失败');
    }

    function runSwitchTfs() {
        runSwitch($scope.switchTfsResult, LogicalBuService.switchRestore, '强切恢复请求失败');
    }

    function runSwitch(state, call, errorTitle, onResult?) {
        var ids = state.logicalBuIds || [];
        if (!ids.length || state.running) {
            return;
        }
        state.running = true;
        call(ids)
            .then(function (result) {
                // unit is logical BU; every BU succeeds or fails on its own
                applyServerScope(state, result);
                state.completed = true;
                state.successCount = result.successCount;
                state.failedCount = result.failedCount;
                state.failureReasons = toFailureReasons(result);
                state.failureDetails = toFailureDetails(result);
                state.buResults = result.results || [];
                if (onResult) {
                    onResult(state, result);
                }
                loadAll();
            }, function (result) {
                // only malformed requests (400) or a dead console end up here
                toastr.error(AppUtil.errorMsg(result), errorTitle);
            })
            .finally(function () {
                state.running = false;
            });
    }
    function selectedLogicalBuIds() {
        // rows are expanded per IDC; a logical BU is always switched as a whole
        var ids = [];
        selectedFilteredLogicalBus().forEach(function (bu) {
            if (bu.id && ids.indexOf(bu.id) < 0) {
                ids.push(bu.id);
            }
        });
        return ids;
    }

    var LAYER_LABELS = {
        CONSOLE: 'Console',
        CONSOLE_DB: '数据库',
        META_SERVER: 'MetaServer',
        KEEPER: 'KeeperContainer'
    };

    var STAGE_LABELS = {
        LOAD: '读取数据',
        UPDATE_DB: '更新 Priority',
        NOTIFY_META: '通知 MetaServer',
        RECOVER_HEALTH: '恢复健康状态'
    };

    function toFailureReasons(result) {
        return (result.summary || []).map(function (item) {
            return {
                layer: LAYER_LABELS[item.layer] || item.layer,
                code: item.code,
                reason: '[' + (LAYER_LABELS[item.layer] || item.layer) + '] ' + item.message,
                count: item.count
            };
        });
    }

    function toFailureDetails(result) {
        var details = [];
        (result.results || []).forEach(function (bu) {
            (bu.failures || []).forEach(function (failure) {
                details.push({
                    logicalBuName: bu.logicalBuName || bu.logicalBuId,
                    layer: LAYER_LABELS[failure.layer] || failure.layer,
                    stage: STAGE_LABELS[failure.stage] || failure.stage,
                    code: failure.code,
                    message: failure.message,
                    target: failure.target,
                    scope: failure.scope,
                    detail: failure.detail
                });
            });
        });
        return details;
    }

    function runRecoverTfs() {
        runSwitch($scope.recoverTfsResult, LogicalBuService.resetHealth, '重置健康状态请求失败', function (state, result) {
            var total = 0, ok = 0;
            (result.results || []).forEach(function (bu) {
                total += bu.keeperCount || 0;
                ok += bu.successKeeperCount || 0;
            });
            state.keeperCount = total;
            state.successKeeperCount = ok;
            // one row per logical BU: name, IDCs, result, recovered / total keepers
            state.keeperRecoverByBu = (result.results || []).map(function (bu) {
                return {
                    logicalBuName: bu.logicalBuName || bu.logicalBuId,
                    idcs: bu.idcs || [],
                    state: bu.state,
                    successContainerCount: bu.successContainerCount || 0,
                    containerCount: bu.containerCount || 0
                };
            });
        });
    }
}
