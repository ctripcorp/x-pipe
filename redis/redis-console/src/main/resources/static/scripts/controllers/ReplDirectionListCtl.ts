angular
    .module('index')
    .controller('ReplDirectionListCtl', ReplDirectionListCtl);

ReplDirectionListCtl.$inject = ['$rootScope', '$scope', 'ReplDirectionService', 'NgTableParams', '$window', 'toastr'];

function ReplDirectionListCtl($rootScope, $scope, ReplDirectionService, NgTableParams, $window, toastr) {
    $scope.originData = []
    $scope.toCompleteReplDirection = [];

    $scope.tableParams = new NgTableParams({}, {});

    $scope.preCompleteReplicationByReplDirection = preCompleteReplicationByReplDirection;
    $scope.doCompleteReplicationByReplDirection = doCompleteReplicationByReplDirection;

    ReplDirectionService.getAllReplDirectionInfos().then(function (response) {
        if (Array.isArray(response)) $scope.originData = response

        $scope.tableParams = new NgTableParams({
            page : 1,
            count : 10,
        }, {
            filterDelay: 100,
            counts: [10, 25, 50],
            dataset: $scope.originData
        });
    })

    function preCompleteReplicationByReplDirection(replDirection) {
        $scope.toCompleteReplDirection = replDirection;
        $('#completeReplicationByReplDirectionConfirm').modal('show');
    }

    function doCompleteReplicationByReplDirection() {
        ReplDirectionService.completeReplicationByReplDirection($scope.toCompleteReplDirection)
            .then(function (result) {
                 $('#completeReplicationByReplDirectionConfirm').modal('hide');
                if (result.message == 'success') {
                     toastr.success("补全成功");
                } else {
                    toastr.error(result.message, "补全失败");
                }
                setTimeout(function () {
                    $window.location.reload();
                },1000);
            });
    }
}