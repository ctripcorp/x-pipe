index_module.controller('ClusterListCtl', ['$scope', '$window', 'AppUtil', 'toastr', 'ClusterService',
    function ($scope, $window, AppUtil, toastr, ClusterService) {


        $scope.deleteCluster = deleteCluster;


        findAllClusters();

        function deleteCluster(cluster) {
            ClusterService.deleteCluster(cluster.clusterName)
                .then(function (result) {
                    location.reload(true);
                }, function (result) {
                    toastr.error(AppUtil.errorMsg(result));
                })
        }


        function findAllClusters() {
            ClusterService.findAllClusters()
                .then(function (result) {
                    $scope.clusters = result;
                }, function (result) {

                });
        }

        setTimeout(function () {
            jQuery(function ($) {
                $('#cluster-list').dataTable({
                    "aoColumns": [
                        {"bSortable": false},
                        null, null,
                        null, null,
                        null,
                        {"bSortable": false}
                    ]
                });

                $('table th input:checkbox').on('click', function () {
                    var that = this;
                    $(this).closest('table').find(
                        'tr > td:first-child input:checkbox')
                        .each(function () {
                            this.checked = that.checked;
                            $(this).closest('tr').toggleClass('selected');
                        });

                });

                $('[data-rel="tooltip"]').tooltip({placement: tooltip_placement});
                function tooltip_placement(context, source) {
                    var $source = $(source);
                    var $parent = $source.closest('table');
                    var off1 = $parent.offset();
                    var w1 = $parent.width();

                    var off2 = $source.offset();
                    var w2 = $source.width();

                    if (parseInt(off2.left) < parseInt(off1.left) + parseInt(
                            w1 / 2)) {
                        return 'right';
                    }
                    return 'left';
                }
            })

        }, 1500);


    }]);
