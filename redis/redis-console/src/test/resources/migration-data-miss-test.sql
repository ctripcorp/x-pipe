insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Lock',1,1);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,1,0);

insert into SHARD_TBL (id,shard_name,cluster_id) values(1,'shard1',1);
insert into SHARD_TBL (id,shard_name,cluster_id) values(2,'shard2',1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,2,1,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (3,1,2,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (4,2,2,2,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(1,'unknown',1,'10.0.0.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(2,'unknown',1,'10.0.0.1',6380,'redis',0,-1,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(3,'unknown',2,'10.0.0.1',6381,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(4,'unknown',2,'10.0.0.1',6382,'redis',0,-1,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(5,'unknown',3,'10.0.0.2',6379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(6,'unknown',3,'10.0.0.2',6380,'redis',0,-1,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(7,'unknown',4,'10.0.0.2',6381,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(8,'unknown',4,'10.0.0.2',6382,'redis',0,-1,null);

insert into MIGRATION_EVENT_TBL (id,event_tag) values (1,'cluster1');

insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id,source_dc_id, destination_dc_id,status) values (1,1,1,1,2,'PartialSuccess');

insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id, log) values (1,1, '{"status":"FAIL","steps":{"CHECK":{"true":"[info][2021-06-07T18:04:34.893]Check success\n"},"MIGRATE_NEW_PRIMARY_DC":{"true":"[info][2021-06-07T18:04:36.291]meta server:{\"ip\":\"127.0.0.1\",\"port\":9747}\n"},"MIGRATE_PREVIOUS_PRIMARY_DC":{"true":"[info][2021-06-07T18:04:35.647]meta server:{\"ip\":\"127.0.0.1\",\"port\":9748}\n"}},"newMaster":{"host":"10.0.0.2","port":6379},"previousPrimaryDcMessage":{"masterAddr":{"host":"10.0.0.1","port":6379},"masterInfo":{"role":"MASTER","masterReplOffset":3010250,"replId":"6870a1ff3ef75f828f77ac664d1fa80eef3b3f04","keeper":false},"message":"[info][2021-06-07T18:04:35.647]meta server:{\"ip\":\"127.0.0.1\",\"port\":9747}\n"}}');
insert into MIGRATION_SHARD_TBL (migration_cluster_id, shard_id, log) values (1,2, '{"status":"SUCCESS","steps":{"CHECK":{"true":"[info][2021-06-07T18:04:35.203]Check success\n"},"MIGRATE_OTHER_DC":{"true":"[info][2021-06-07T18:04:36.776]meta server:{\"ip\":\"127.0.0.1\",\"port\":9747}\n"},"MIGRATE_PREVIOUS_PRIMARY_DC":{"true":"[info][2021-06-07T18:04:35.437]meta server:{\"ip\":\"127.0.0.1\",\"port\":9747}\n"},"MIGRATE":{"true":"[info][2021-06-07T18:04:37.008]Success\n"},"MIGRATE_NEW_PRIMARY_DC":{"true":"[info][2021-06-07T18:04:36.021]meta server:{\"ip\":\"127.0.0.1\",\"port\":9748}\n"}},"newMaster":{"host":"10.0.0.2","port":6381},"previousPrimaryDcMessage":{"masterAddr":{"host":"10.0.0.1","port":6381},"masterInfo":{"role":"MASTER","masterReplOffset":2732858,"replId":"33dc21843c4937627fb3cb4ad3bf48488114900a","keeper":false},"message":"[info][2021-06-07T18:04:35.437]meta server:{\"ip\":\"127.0.0.1\",\"port\":9747}\n"}}');