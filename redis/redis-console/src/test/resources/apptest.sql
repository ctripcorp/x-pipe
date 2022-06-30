insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id,cluster_type) values
                        (5,'singleDcCluster',1,'Cluster:singleDcCluster , ActiveDC : A','0000000000000000','Normal',1, 1,'SINGLE_DC');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,status,is_xpipe_interested, cluster_org_id,cluster_type) values
(6,'credis_trocks_test',0,'Normal',1, 1,'CROSS_DC');

insert into redis_check_rule_tbl (id,check_type,param,description) values (1,'config', '{\"configName\" : \"repl-backlog-size\", \"expectedVaule\" : \"128\"}', 'rule1');
insert into redis_check_rule_tbl (id,check_type,param,description) values (2,'config', '{\"configName\" : \"repl-backlog-size\", \"expectedVaule\" : \"256\"}', 'rule2');

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (10,1,5,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (11,1,6,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (12,2,6,1,0);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(8,'shard8','shard8', 5);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(9,'credis_trocks_test_1','credis_trocks_test_1', 6);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (17,10,8,5,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (18,11,9,4,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (19,12,9,4,1);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',1, 1,'1,2');

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (7,3,1,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(1,'shard1','shard1', 1);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(2,'shard2','shard2', 1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,2,1,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (3,1,2,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (4,2,2,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (13,7,1,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (14,7,2,2,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(1,'ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6000,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(2,'ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6001,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(3,'unknown',1,'127.0.0.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(4,'unknown',1,'127.0.0.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_active) values(6,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6100,'keeper',0,0,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(7,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',2,'127.0.0.1',6101,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(8,'unknown',2,'127.0.0.1',6579,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(9,'unknown',2,'127.0.0.1',6679,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_active) values(10,'bfffffffffffffffffffffffffffffffffffffff',3,'127.0.0.1',7000,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(11,'bfffffffffffffffffffffffffffffffffffffff',3,'127.0.0.1',7001,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(12,'unknown',3,'127.0.0.1',7379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(13,'unknown',3,'127.0.0.1',7479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_active) values(15,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',4,'127.0.0.1',7100,'keeper',0,0,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(16,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',4,'127.0.0.1',7101,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(17,'unknown',4,'127.0.0.1',7579,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(18,'unknown',4,'127.0.0.1',7679,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_active) values(19,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',13,'127.0.0.3',7000,'keeper',0,0,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(20,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',13,'127.0.0.1',8001,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(21,'unknown',13,'127.0.0.2',8379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(22,'unknown',13,'127.0.0.2',8479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id,keeper_active) values(23,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',14,'127.0.0.3',7100,'keeper',0,0,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(24,'beeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',14,'127.0.0.3',8101,'keeper',0,0,5);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(25,'unknown',14,'127.0.0.2',8579,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(26,'unknown',14,'127.0.0.2',8679,'redis',0,0,null);


insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Normal',1, 2);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (3,1,2,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (4,2,2,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(3,'cluster2shard1','cluster2shard1', 2);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(4,'cluster2shard2','cluster2shard2', 2);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (5,3,3,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (6,4,3,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (7,3,4,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (8,4,4,2,1);

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, keepercontainer_org_id) values (7,1,'127.0.1.4',7083,1,2);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, keepercontainer_org_id) values (8,1,'127.0.1.5',7084,1,2);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, keepercontainer_org_id) values (9,2,'127.0.1.6',7083,1,2);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, keepercontainer_org_id) values (10,2,'127.0.1.7',7084,1,2);


insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (3,'cluster3',2,'Cluster:cluster3 , ActiveDC : B','0000000000000000','Normal',1, 3);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (5,1,3,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (6,2,3,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(5,'cluster3shard1','cluster3shard1', 3);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(6,'cluster3shard2','cluster3shard2', 3);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (9, 5,5,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (10,6,5,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (11,5,6,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (12,6,6,2,1);

insert into organization_tbl(org_id, org_name) values (1, 'org-1'), (2, 'org-2'), (3, 'org-3'), (4, 'org-4'), (5, 'org-5'), (6, 'org-6'), (7, 'other-1'), (999,'other-2');

insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (1,3,'PROXYTCP://172.19.0.20:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (2,1,'PROXYTLS://172.19.0.21:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (3,3,'PROXYTCP://172.19.0.22:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (4,1,'PROXYTCP://172.19.0.23:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (5,1,'PROXYTCP://172.19.0.24:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (6,2,'PROXYTCP://172.19.0.25:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (7,2,'PROXYTCP://172.19.0.26:443',1,1,0);

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (1, 3, 3, 1, '1,3', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (2, 2, 3, 1, '3', '6', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (3, 8, 3, 1, '', '', '4', 1, 'console', 0, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (4, 4, 3, 1, '', '5', 1, 'console', 1, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (5, 2, 3, 2, '3', '6', 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (6, 8, 3, 2, '1', '7', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (7, 7, 3, 2, '1', '6', 1, 'meta', 0, 'desc');

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (8, 0, 3, 1, '1,3', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (9, 0, 3, 1, '3', '6', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (10, 0, 3, 1, '', '', '4', 1, 'console', 0, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (11, 0, 3, 1, '', '5', 1, 'meta', 1, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (12, 0, 3, 2, '3', '6', 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (13, 0, 3, 2, '1', '7', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (14, 0, 3, 2, '1', '6', 1, 'meta', 0, 'desc');

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_type, cluster_designated_route_ids) values (4,'bi-cluster1',0,'bi-cluster:cluster1 , ActiveDC : B','0000000000000000','Normal',1, 4, 'BI_DIRECTION', '1,2');

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,active_redis_check_rules) values (8,1,4,1,0,'1,2');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,active_redis_check_rules) values (9,2,4,1,0,'1,2');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(7,'bicluster1shard1','bicluster1shard1', 4);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (15, 8,7,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (16, 9,7,2,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(27,'unknown',15,'127.0.0.3',8579,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(28,'unknown',15,'127.0.0.3',8679,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(29,'unknown',16,'127.0.0.3',8779,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(30,'unknown',16,'127.0.0.3',8879,'redis',0,0,null);

insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master) values('unknown',18,'10.2.27.55',6399,'redis',1,0);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master) values('unknown',18,'10.2.27.51',6399,'redis',0,0);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master) values('unknown',19,'10.2.27.53',6399,'redis',0,0);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master) values('unknown',19,'10.2.27.54',6399,'redis',0,0);

insert into SENTINEL_GROUP_TBL (sentinel_group_id,cluster_type) values (4,'CROSS_DC');

insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,4,'127.0.0.0',7000);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (2,4,'127.0.0.0',7001);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (3,4,'127.0.0.0',7002);

insert into SENTINEL_GROUP_TBL (sentinel_group_id,cluster_type) values (5,'SINGLE_DC');

insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,5,'127.0.0.1',8000);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,5,'127.0.0.1',8001);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,5,'127.0.0.1',8002);

insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',17,'127.0.0.1',9999,'redis',0,0,null);
insert into REDIS_TBL (run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values('unknown',17,'127.0.0.1',9990,'redis',1,0,null);

insert into AZ_TBL (id, dc_id, az_name, active, description) values (1, 3, 'A', 1, 'zone for dc:fra zone A');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (2, 3, 'B', 1, 'zone for dc:fra zone B');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (3, 3, 'C', 0, 'zone for dc:fra zone C');

insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(1,3,1,'127.0.0.1','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(2,3,1,'127.0.0.2','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(3,3,2,'127.0.0.3','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(4,3,2,'127.0.0.4','8080',1,0);

insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(5,2,0,'127.0.0.5','8080',1,2);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(6,2,0,'127.0.0.6','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(7,2,0,'127.0.0.7','8080',0,2);

insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(8,1,0,'127.0.0.8','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(9,1,0,'127.0.0.9','8080',1,0);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (7,'hetero-cluster',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'hetero');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(21,'hetero-cluster_1','hetero-cluster_1', 7);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(22,'hetero-cluster_2','hetero-cluster_2', 7);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(23,'hetero-cluster_fra_1','hetero-cluster_fra_1', 7);


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (31,1,7,1,0,'jq',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (32,2,7,1,0,'oy',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (33,3,7,1,0,'fra',0);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (51,31,21,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (52,31,22,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (53,32,21,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (54,32,22,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (55,33,23,3,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(61,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(62,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(63,'unknown',51,'127.0.1.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(64,'unknown',51,'127.0.1.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(65,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(66,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(67,'unknown',52,'127.0.2.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(68,'unknown',52,'127.0.2.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(69,'ffffffffffffffffffffffffffffffffffffffff',53,'127.0.3.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(70,'ffffffffffffffffffffffffffffffffffffffff',53,'127.0.3.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(71,'unknown',53,'127.0.3.1',6379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(72,'unknown',53,'127.0.3.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(73,'ffffffffffffffffffffffffffffffffffffffff',54,'127.0.4.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(74,'ffffffffffffffffffffffffffffffffffffffff',54,'127.0.4.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(75,'unknown',54,'127.0.4.1',6379,'redis',0,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(76,'unknown',54,'127.0.4.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(77,'unknown',55,'127.0.5.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(78,'unknown',55,'127.0.5.1',6479,'redis',0,0,null);

insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(1, 21, 2, '127.0.0.1', 16000, 1, 1);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(2, 21, 2, '127.0.0.2', 16000, 0, 2);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(1, 7, 1, 1, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(2, 7, 1, 1, 3);


insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type, cluster_admin_emails) values (8,'hetero-cluster2',2,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 2,'', 'hetero', 'test@111.com');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(24,'hetero-cluste1r_1','hetero-cluster_1', 8);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(25,'hetero-cluster1_2','hetero-cluster_2', 8);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(26,'hetero-cluster1_fra_1','hetero-cluster_fra_1', 8);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (34,1,8,1,0,'jq',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (35,2,8,1,0,'oy',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (36,3,8,1,0,'fra',0);


insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (56,34,24,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (57,34,25,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (58,35,24,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (59,35,25,2,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (60,36,26,3,1);

insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(5, 24, 4, '127.0.0.1', 16002, 1, 1);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(6, 24, 4, '127.0.0.2', 16002, 0, 2);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(7, 25, 4, '127.0.0.1', 16003, 1, 1);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(8, 25, 4, '127.0.0.2', 16003, 0, 2);


insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(3, 8, 1, 1, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(4, 8, 1, 1, 3);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(5, 8, 2, 2, 3);