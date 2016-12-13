insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Migrating');

insert into DC_CLUSTER_TBL (dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,0);
insert into DC_CLUSTER_TBL (dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,1,1,0);
insert into DC_CLUSTER_TBL (dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,2,1,0);
insert into DC_CLUSTER_TBL (dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,0);

insert into SHARD_TBL (shard_name,cluster_id) values('shard1',1);
insert into SHARD_TBL (shard_name,cluster_id) values('shard2',1);
insert into SHARD_TBL (id,shard_name,cluster_id) values(3,'shard1',2);
insert into SHARD_TBL (id,shard_name,cluster_id) values(4,'shard2',2);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,1,2,1);

insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6000,'keeper',0,-1,1);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6000,'keeper',0,-1,2);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',1,'127.0.0.1',6379,'redis',1,0,null);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',1,'127.0.0.1',6379,'redis',0,-1,null);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',1,'127.0.0.1',6379,'redis',0,-1,null);

insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6000,'keeper',0,-1,4);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6000,'keeper',0,-1,5);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',2,'127.0.0.1',6379,'redis',0,-1,null);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',2,'127.0.0.1',6379,'redis',0,-1,null);

insert into MIGRATION_EVENT_TBL (id,event_tag) values (2,'xpipe-test-event-2');

insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (2,2,2,2,'Checking');

insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (2,3);
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id) values (2,4);
