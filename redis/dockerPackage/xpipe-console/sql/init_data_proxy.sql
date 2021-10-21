insert into ZONE_TBL (id,zone_name) values(1,'SHA');
insert into ZONE_TBL (id,zone_name) values(2,'FRA');

insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (1,1,'jq',1,'DC:jq','0000000000000000');
insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (2,1,'oy',1,'DC:oy','0000000000000000');
insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (3,2,'fra',1,'DC:AWS-FRA','0000000000000000');

insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(1,1,'127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002','setinel no.1');
insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(2,2,'127.0.0.1:17172,127.0.0.1:17172','setinel no.2');
insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(3,3,'127.0.0.1:32222,127.0.0.1:32223','setinel no.3');

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (1,1,'172.19.0.7',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (2,1,'172.19.0.27',8080,1);

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (4,2,'172.19.0.8',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (5,2,'172.19.0.28',8080,1);


insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',1);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(1,'shard1','shard1', 1);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(2,'shard2','shard2', 1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,2,1,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (3,1,2,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (4,2,2,2,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(1,'ffffffffffffffffffffffffffffffffffffffff',1,'172.19.0.27',6380,'keeper',0,0,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(2,'ffffffffffffffffffffffffffffffffffffffff',1,'172.19.0.7',6380,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(3,'unknown',1,'172.19.0.10',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(4,'unknown',1,'172.19.0.11',6379,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(6,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'172.19.0.28',6380,'keeper',0,0,4);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(7,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'172.19.0.8',6380,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(8,'unknown',2,'172.19.0.12',6379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(9,'unknown',2,'172.19.0.13',6379,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(10,'bfffffffffffffffffffffffffffffffffffffff',3,'172.19.0.27',6381,'keeper',0,0,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(11,'bfffffffffffffffffffffffffffffffffffffff',3,'172.19.0.7',6381,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(12,'unknown',3,'172.19.0.14',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(13,'unknown',3,'172.19.0.15',6379,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(15,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',4,'172.19.0.28',6381,'keeper',0,0,4);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(16,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',4,'172.19.0.8',6381,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(17,'unknown',4,'172.19.0.16',6379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(18,'unknown',4,'172.19.0.17',6379,'redis',0,0,null);


insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (1,1,'PROXYTCP://172.19.0.20:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (2,1,'PROXYTLS://172.19.0.20:443',1,1,0);

insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (3,2,'PROXYTCP://172.19.0.21:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (4,2,'PROXYTLS://172.19.0.21:443',1,1,0);

insert into route_tbl (id,src_dc_id,dst_dc_id,src_proxy_ids,dst_proxy_ids,active,tag) values(1,1,2,'1','4',1,'meta');
insert into route_tbl (id,src_dc_id,dst_dc_id,src_proxy_ids,dst_proxy_ids,active,tag) values(2,2,1,'3','2',1,'meta');