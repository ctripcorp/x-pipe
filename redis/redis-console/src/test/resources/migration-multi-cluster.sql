insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested, cluster_org_id) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Lock',1, 1, 1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested, cluster_org_id) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Lock',1, 1, 1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested, cluster_org_id) values (3,'cluster3',1,'Cluster:cluster3 , ActiveDC : A','0000000000000000','Lock',1, 1, 1);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(1,'shard1','shard1', 1);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(2,'shard2','shard2', 1);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(3,'shard1','shard1', 2);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(4,'shard2','shard2', 2);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(5,'shard2','shard2', 3);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(6,'shard1','shard1', 3);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (3,1,2,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (4,2,2,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (5,1,3,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (6,2,3,1,0);

insert into MIGRATION_EVENT_TBL (id,event_tag) values (1,'migration-1');

insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id,source_dc_id,destination_dc_id,status) values (1,1,1,1,2,'Initiated');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id,source_dc_id,destination_dc_id,status) values (2,1,2,1,2,'Initiated');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id,source_dc_id,destination_dc_id,status) values (3,1,3,1,2,'Initiated');

insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (1,1);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (1,2);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (2,3);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (2,4);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (3,5);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (3,6);
