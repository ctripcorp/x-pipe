services.service('MigrationService', ['$resource', '$q', function($resource, $q) {
	var resource = $resource('', {}, {
		create_event: {
			method : 'POST',
			url : '/console/migration/events'
		},
		find_all: {
			method : 'GET',
			url : '/console/migration/events/all',
			isArray : true
		},
		find_event_details: {
			method : 'GET',
			url : '/console/migration/events/:eventId',
			isArray : true
		},
		continue_migration_cluster: {
			method : 'POST',
			url : '/console/migration/events/:eventId/clusters/:clusterId'
		},
		cancel_migration_cluster: {
			method: 'POST',
			url : '/console/migration/events/:eventId/clusters/:clusterId/cancel'
		},
		rollback_migration_cluster: {
			method: 'POST',
			url: '/console/migration/events/:eventId/clusters/:clusterId/rollback'
		},
		forcepublish_migration_cluster: {
			method: 'POST',
			url: '/console/migration/events/:eventId/clusters/:clusterId/forcePublish'
		},
		forceend_migration_cluster: {
			method: 'POST',
			url: '/console/migration/events/:eventId/clusters/:clusterId/forceEnd'
		},
		check_migration_system: {
			method: 'GET',
			url: '/console/migration/system/health/status'
		},
		get_default_migrate_cluster: {
			method: 'GET',
			url: '/console/migration/default/cluster'
		}
	});

	function createEvent(migrationClusters) {
		var d = $q.defer();
		resource.create_event({},
				{
					event : {
						migrationClusters : migrationClusters
					}
				},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function findAll() {
		var d = $q.defer();
		resource.find_all({},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function findEventDetails(eventId) {
		var d = $q.defer();
		resource.find_event_details(
			{
				eventId : eventId
			},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function continueMigrationCluster(eventId, clusterId) {
		var d = $q.defer();
		resource.continue_migration_cluster(
			{
				eventId : eventId,
				clusterId : clusterId
			},
			{},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}
	
	function cancelMigrationCluster(eventId, clusterId) {
		var d = $q.defer();
		resource.cancel_migration_cluster(
				{
					eventId : eventId,
					clusterId : clusterId
				},
				{},
				function(result) {
					return d.resolve(result);
				},
				function(result) {
					return d.reject(result);
				});
		return d.promise;
	}

    function rollbackMigrationCluster(eventId, clusterId) {
        var d = $q.defer();
        resource.rollback_migration_cluster(
            {
                eventId : eventId,
                clusterId : clusterId
            },
            {},
            function(result) {
                return d.resolve(result);
            },
            function(result) {
                return d.reject(result);
            });
        return d.promise;
    }
    
    function forcePublishMigrationCluster(eventId, clusterId) {
        var d = $q.defer();
        resource.forcepublish_migration_cluster(
            {
                eventId : eventId,
                clusterId : clusterId
            },
            {},
            function(result) {
                return d.resolve(result);
            },
            function(result) {
                return d.reject(result);
            });
        return d.promise;
    }
    
    function forceEndMigrationCluster(eventId, clusterId) {
        var d = $q.defer();
        resource.forceend_migration_cluster(
            {
                eventId : eventId,
                clusterId : clusterId
            },
            {},
            function(result) {
                return d.resolve(result);
            },
            function(result) {
                return d.reject(result);
            });
        return d.promise;
    }

    function checkMigrationSystem() {
        var d = $q.defer();
        resource.check_migration_system({},
            function(result) {
                d.resolve(result);
            },
            function(result) {
                d.reject(result);
            });
        return d.promise;
    }

    function getDefaultMigrationCluster() {
        var d = $q.defer();
        resource.get_default_migrate_cluster({},
            function(result) {
                d.resolve(result);
            },
            function(result) {
                d.reject(result);
            });
        return d.promise;
    }

	return {
		createEvent : createEvent,
		findAll : findAll,
		findEventDetails : findEventDetails,
		continueMigrationCluster : continueMigrationCluster,
		cancelMigrationCluster : cancelMigrationCluster,
		rollbackMigrationCluster: rollbackMigrationCluster,
		forcePublishMigrationCluster : forcePublishMigrationCluster,
		forceEndMigrationCluster : forceEndMigrationCluster,
        checkMigrationSystem : checkMigrationSystem,
        getDefaultMigrationCluster : getDefaultMigrationCluster
	}
}]);