insert into DC_TBL (dc_name,dc_active,dc_description,dc_last_modified_time) values ('NTGXH',1,'DC:NTGXH','2016080913560001');
insert into DC_TBL (dc_name,dc_active,dc_description,dc_last_modified_time) values ('FAT',1,'DC:FAT','2016080913560001');

insert into METASERVER_TBL (metaserver_name,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('NTGXH_meta1',1,'10.2.38.87',8080,1,'master');
insert into METASERVER_TBL (metaserver_name,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('NTGXH_meta2',1,'10.2.38.88',8080,1,'slave');
insert into METASERVER_TBL (metaserver_name,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('NTGXH_meta3',1,'10.2.38.89',8080,1,'slave');
insert into METASERVER_TBL (metaserver_name,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('FAT_meta1',2,'10.2.38.90',8080,1,'master');
insert into METASERVER_TBL (metaserver_name,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('FAT_meta2',2,'10.2.38.45',8080,1,'slave');

insert into SETINEL_TBL (dc_id,setinel_address,setinel_description) values(1,'127.0.0.1:17171,127.0.0.1:17171','setinel no.1');
insert into SETINEL_TBL (dc_id,setinel_address,setinel_description) values(1,'127.0.0.1:17171,127.0.0.1:17171','setinel no.2');

insert into CLUSTER_TBL (cluster_name,activedc_id,cluster_description,cluster_last_modified_time) values ('cluster1',1,'Cluster:cluster1 , ActiveDC : NTGXH','2016080914030001');
insert into CLUSTER_TBL (cluster_name,activedc_id,cluster_description,cluster_last_modified_time) values ('cluster2',2,'Cluster:cluster2 , ActiveDC : FAT','2016080914030001');

insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id,dc_cluster_phase) values (1,1,1,1);
insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id,dc_cluster_phase) values (2,1,3,1); 

insert into SHARD_TBL (shard_name,cluster_id) values('shard1',1);
insert into SHARD_TBL (shard_name,cluster_id) values('shard2',1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,1,2,1);

insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('ffffffffffffffffffffffffffffffffffffffff',1,'10.2.38.11',6000,'keeper',1,3,1);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('ffffffffffffffffffffffffffffffffffffffff',1,'10.2.38.12',6000,'keeper',0,1,2);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('redis3',1,'10.2.58.242',6379,'redis',0,null,null);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('redis4',1,'10.2.58.243',6379,'redis',0,3,null);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('redis5',1,'10.2.58.244',6379,'redis',0,3,null);

insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'10.2.38.13',6000,'keeper',1,1,3);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'10.2.38.46',6000,'keeper',0,6,4);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('redis8',2,'10.3.2.23',6379,'redis',0,6,null);
insert into REDIS_TBL (redis_name,dc_cluster_shard_id,redis_ip,redis_port,redis_role,keeper_active,redis_master,keepercontainer_id) values('redis9',2,'10.3.2.220',6379,'redis',0,6,null);

insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (1,'10.2.38.11',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (1,'10.2.38.12',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (2,'10.2.38.13',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (2,'10.2.38.46',8080,1);

