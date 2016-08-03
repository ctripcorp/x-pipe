insert into DC_TBL (dc_id,dc_active,dc_description,dc_last_modified_time) values ('jq',1,'dc named jq located in shanghai.','1234567');
insert into DC_TBL (dc_id,dc_active,dc_description,dc_last_modified_time) values ('oy',1,'dc named oy located in shanghai.','1234567');

insert into METASERVER_TBL (metaserver_id,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('jq_meta_1','jq','1.1.1.1',9747,1,'master');
insert into METASERVER_TBL (metaserver_id,dc_id,metaserver_ip,metaserver_port,metaserver_active,metaserver_role) values ('jq_meta_2','jq','1.1.1.1',9748,1,'slave');

insert into SETINEL_TBL (dc_id,setinel_address,setinel_description) values('jq','127.0.0.1:17171,127.0.0.1:17171','setinel no.1');
insert into SETINEL_TBL (dc_id,setinel_address,setinel_description) values('jq','127.0.0.1:17171,127.0.0.1:17171','setinel no.2');

insert into CLUSTER_TBL (cluster_id,activedc_id,cluster_description,cluster_last_modified_time) values ('cluster1','jq','Cluster 1 located in JQ','1234567');
insert into CLUSTER_TBL (cluster_id,activedc_id,cluster_description,cluster_last_modified_time) values ('cluster2','oy','Cluster 1 located in OY','1234567');

insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id,dc_cluster_phase) values ('jq', 'cluster1','jq_meta_1',1);
insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id,dc_cluster_phase) values ('oy', 'cluster1','jq_meta_2',1); 

insert into SHARD_TBL (shard_id,cluster_id) values('shard1','cluster1');
insert into SHARD_TBL (shard_id,cluster_id) values('shard2','cluster1');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,setinel_monitor_name,dc_cluster_shard_phase) values (0,'shard1','setinel1','setinel1_monitor_1',1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id,setinel_id,setinel_monitor_name,dc_cluster_shard_phase) values (0,'shard2','setinel2','setinel2_monitor_2',1);

insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('40a',0,'1.1.1.1',8888,'keeper',1,'1.1.1.1:1234',1);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('40b',0,'1.1.1.2',9999,'keeper',0,'1.1.1.1:8888',1);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('40c',0,'1.1.1.3',1234,'master',1,'',null);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('40d',0,'1.1.1.4',1234,'slave',1,'1.1.1.3:1234',null);

insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('41a',1,'1.1.1.1',8888,'keeper',1,'1.1.1.1:1234',1);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('41b',1,'1.1.1.2',9999,'keeper',0,'1.1.1.1:8888',1);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('41c',1,'1.1.1.3',1234,'master',1,'',null);
insert into REDIS_TBL (redis_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,redis_active,redis_master,keepercontainer_id) values('41d',1,'1.1.1.4',1234,'slave',1,'1.1.1.3:1234',null);

insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values ('jq','1.1.1.1',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values ('jq','1.1.1.1',8081,1);

