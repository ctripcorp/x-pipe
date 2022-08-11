angular
	.module('services')
	.service('MigrationService', MigrationService);

MigrationService.$inject = ['$resource', '$q'];

function MigrationService($resource, $q) {

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
		find: {
			method : 'GET',
			url : '/console/migration/events',
		},
		findByOperator: {
			method : 'GET',
			url : '/console/migration/events/by/operator',
		},
		findByMigrationStatusType: {
			method : 'GET',
			url : '/console/migration/events/by/migration/status/type',
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
		force_process_migration_cluster: {
			method: 'POST',
			url: '/console/migration/events/:eventId/clusters/:clusterId/forceProcess'
		},
		force_end_migration_cluster: {
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

	function find(page, size, clusterName = null) {
		let d = $q.defer();
		resource.find({ page, size, clusterName},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function findByOperator(page, size, operator) {
		let d = $q.defer();
		resource.findByOperator({ page, size, operator},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function findByMigrationStatusType(page, size, type) {
		let d = $q.defer();
		resource.findByMigrationStatusType({ page, size, type},
			function(result) {
				d.resolve(result);
			},
			function(result) {
				d.reject(result);
			});
		return d.promise;
	}

	function findWithoutTestClusters(page, size) {
		let d = $q.defer();
		resource.find({ page, size, withoutTestClusters: true},
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

    function forceProcessMigrationCluster(eventId, clusterId) {
        var d = $q.defer();
        resource.force_process_migration_cluster(
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
        resource.force_end_migration_cluster(
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
		find: find,
		findByOperator: findByOperator,
		findByMigrationStatusType: findByMigrationStatusType,
		findWithoutTestClusters: findWithoutTestClusters,
		findEventDetails : findEventDetails,
		continueMigrationCluster : continueMigrationCluster,
		cancelMigrationCluster : cancelMigrationCluster,
		rollbackMigrationCluster: rollbackMigrationCluster,
		forceSkipChecking : forceProcessMigrationCluster,
		forcePublish : forceProcessMigrationCluster,
		forceProcessMigrationCluster : forceProcessMigrationCluster,
		forceEndMigrationCluster : forceEndMigrationCluster,
        checkMigrationSystem : checkMigrationSystem,
        getDefaultMigrationCluster : getDefaultMigrationCluster
	}
}