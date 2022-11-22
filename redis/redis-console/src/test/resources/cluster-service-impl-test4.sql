

insert into AZ_TBL (id, dc_id, az_name, active, description) values (1, 3, 'A', 1, 'zone for dc:fra zone A');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (2, 3, 'B', 1, 'zone for dc:fra zone B');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (3, 3, 'C', 0, 'zone for dc:fra zone C');

insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (4,1,'rb',1,'DC:rb','0000000000000000');
-- 增加 appliercontainer
-- jq
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(41,1,0,'127.0.0.41','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(42,1,0,'127.0.0.42','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(43,1,0,'127.0.0.43','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(44,1,0,'127.0.0.44','8080',1,0);
-- oy
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(45,2,0,'127.0.0.45','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(46,2,0,'127.0.0.46','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(47,2,0,'127.0.0.47','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(48,2,0,'127.0.0.48','8080',1,0);
-- fra
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(51,3,1,'127.0.0.51','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(52,3,1,'127.0.0.52','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(53,3,2,'127.0.0.53','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(54,3,2,'127.0.0.54','8080',1,0);
-- rb
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(55,4,0,'127.0.0.55','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(56,4,0,'127.0.0.56','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(57,4,0,'127.0.0.57','8080',1,0);
insert into APPLIERCONTAINER_TBL(appliercontainer_id, appliercontainer_dc, appliercontainer_az, appliercontainer_ip, appliercontainer_port, appliercontainer_active, appliercontainer_org) values(58,4,0,'127.0.0.58','8080',1,0);

-- 增加 keepercontainer
-- jq
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (161,1,'127.0.0.61',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (162,1,'127.0.0.62',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (163,1,'127.0.0.63',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (164,1,'127.0.0.64',8080,1,0);
-- oy
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (165,2,'127.0.0.65',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (166,2,'127.0.0.66',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (167,2,'127.0.0.67',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (168,2,'127.0.0.68',8080,1,0);
-- fra
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (171,3,'127.0.0.71',8080,1,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (172,3,'127.0.0.72',8080,1,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (173,3,'127.0.0.73',8080,1,2);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (174,3,'127.0.0.74',8080,1,2);
-- rb
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (175,4,'127.0.0.75',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (176,4,'127.0.0.76',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (177,4,'127.0.0.77',8080,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (178,4,'127.0.0.78',8080,1,0);


-- 增加hetero集群
-- 增加 repl-hetero-cluster1
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (21,'repl-hetero-cluster1',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'one_way');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(41,'repl-hetero-cluster1_1','repl-hetero-cluster1_1', 21);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(42,'repl-hetero-cluster1_2','repl-hetero-cluster1_2', 21);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(43,'repl-hetero-cluster1_fra_1','repl-hetero-cluster1_fra_1', 21);


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (131,1,21,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (132,2,21,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (133,3,21,1,0,'fra','MASTER');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (151,131,41,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (152,131,42,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (153,132,41,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (154,132,42,2,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (155,133,43,3,1);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(11, 21, 0, 0, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(12, 21, 0, 0, 3);

-- 增加 repl-hetero-cluster2 纯中转机房
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (22,'repl-hetero-cluster2',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'one_way');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(51,'repl-hetero-cluster2_1','repl-hetero-cluster2_1', 22);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(52,'repl-hetero-cluster2_2','repl-hetero-cluster2_2', 22);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(53,'repl-hetero-cluster2_3','repl-hetero-cluster2_3', 22);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(54,'repl-hetero-cluster2_fra_1','repl-hetero-cluster2_fra_1', 22);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(55,'repl-hetero-cluster2_fra_2','repl-hetero-cluster2_fra_2', 22);


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (141,1,22,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (142,2,22,1,0,'oy','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (143,3,22,1,0,'fra','MASTER');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (161,141,51,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (162,141,52,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (163,141,53,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (165,143,54,3,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (166,143,55,3,1);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(13, 22, 0, 0, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(14, 22, 0, 2, 3);

-- 增加 repl-hetero-cluster3  非纯中转机房
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (23,'repl-hetero-cluster3',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'one_way');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(61,'repl-hetero-cluster3_1','repl-hetero-cluster2_1', 23);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(62,'repl-hetero-cluster3_2','repl-hetero-cluster2_2', 23);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(63,'repl-hetero-cluster3_3','repl-hetero-cluster2_3', 23);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(64,'repl-hetero-cluster3_fra_1','repl-hetero-cluster3_fra_1', 23);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(65,'repl-hetero-cluster3_fra_2','repl-hetero-cluster3_fra_2', 23);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(66,'repl-hetero-cluster3_oy_1','repl-hetero-cluster3_oy_1', 23);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (151,1,23,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (152,2,23,1,0,'oy','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (153,3,23,1,0,'fra','MASTER');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (171,151,61,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (172,151,62,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (173,151,63,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (175,153,64,3,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (176,153,65,3,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (177,152,66,2,1);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(15, 23, 0, 0, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(16, 23, 0, 2, 3);

-- 增加 repl-hetero-cluster4 jq-->rb-->oy-->fra  纯中转机房
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (24,'repl-hetero-cluster4',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'one_way');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(71,'repl-hetero-cluster4_1','repl-hetero-cluster4_1', 24);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(72,'repl-hetero-cluster4_2','repl-hetero-cluster4_2', 24);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(73,'repl-hetero-cluster4_3','repl-hetero-cluster4_3', 24);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(74,'repl-hetero-cluster4_fra_1','repl-hetero-cluster4_fra_1', 24);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(75,'repl-hetero-cluster4_fra_2','repl-hetero-cluster4_fra_2', 24);


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (161,1,24,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (162,2,24,1,0,'oy','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (163,3,24,1,0,'fra','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (164,4,24,1,0,'rb','MASTER');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (181,161,71,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (182,161,72,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (183,161,73,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (185,163,74,3,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (186,163,75,3,1);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(17, 24, 0, 0, 4);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(18, 24, 0, 4, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(19, 24, 0, 2, 3);

-- 增加 repl-hetero-cluster5  jq-->rb-->oy-->fra 非纯中转机房
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (25,'repl-hetero-cluster5',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'one_way');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(81,'repl-hetero-cluster5_1','repl-hetero-cluster5_1', 25);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(82,'repl-hetero-cluster5_2','repl-hetero-cluster5_2', 25);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(83,'repl-hetero-cluster5_3','repl-hetero-cluster5_3', 25);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(84,'repl-hetero-cluster5_fra_1','repl-hetero-cluster5_fra_1', 25);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(85,'repl-hetero-cluster5_fra_2','repl-hetero-cluster5_fra_2', 25);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(86,'repl-hetero-cluster5_oy_1','repl-hetero-cluster5_oy_1', 25);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(87,'repl-hetero-cluster5_rb_1','repl-hetero-cluster5_rb_1', 25);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (171,1,25,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (172,2,25,1,0,'oy','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (173,3,25,1,0,'fra','MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (174,4,25,1,0,'rb','MASTER');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (191,171,81,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (192,171,82,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (193,171,83,1,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (195,173,84,3,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (196,173,85,3,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (197,174,87,4,1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (198,172,86,2,1);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(21, 25, 0, 0, 4);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(22, 25, 0, 4, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(23, 25, 0, 2, 3);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(24, 25, 2, 2, 3);