services.service('DcService', ['$resource', '$q', function ($resource, $q) {
    
    var resource = $resource('', {}, {
        load_all_dc: {
            method: 'GET',
            url: '/console/dcs/all',
            isArray : true
        },
        delete_dc: {
        	method: 'DELETE',
            url: '/console/clusters/:clusterName/dcs/:dcName'
        },
        add_dc: {
            method: 'POST',
            url: '/console/clusters/:clusterName/dcs/:dcName'
        },
        find_all_dc_rich_info:{
            method: 'GET',
            url: '/console/dcs/all/richInformation',
            isArray: true
        }
    });

    function addDc(clusterName,dcName) {
    	var d = $q.defer();
        resource.add_dc({
            clusterName : clusterName,             
        	dcName: dcName
                              },
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }
    
    function deleteDc(clusterName,dcName) {
    	var d = $q.defer();
        resource.delete_dc({
            clusterName : clusterName,                      
        	dcName: dcName
                              },
                              function (result) {
                                  d.resolve(result);
                              }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }
    
    function loadAllDcs() {
    	var d = $q.defer();
        resource.load_all_dc({},
                                   function (result) {
                                       d.resolve(result);
                                   }, function (result) {
                d.reject(result);
            });
        return d.promise;
    }

    function findAllDcsRichInfo() {
        var d = $q.defer();
        resource.find_all_dc_rich_info({},
                function (result) {
                    d.resolve(result);
                },
                function (result) {
                    d.reject(result);
                }
            );
        return d.promise;
    }
   
    return {
    	loadAllDcs : loadAllDcs,
    	deleteDc : deleteDc,
    	addDc : addDc,
        findAllDcsRichInfo: findAllDcsRichInfo
    }
}]);
