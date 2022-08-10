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

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (11,1,'127.0.0.11',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (12,1,'127.0.0.12',8080,1);

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (13,2,'127.0.0.13',8080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (14,2,'127.0.0.14',8080,1);

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (15,3,'127.0.0.15',8080,1,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (16,3,'127.0.0.16',8080,1,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (17,3,'127.0.0.17',8080,1,2);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active, az_id) values (18,3,'127.0.0.18',8080,1,2);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (7,'hetero-cluster',1,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 1,'', 'hetero');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(21,'hetero-cluster_1','hetero-cluster_1', 7);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(22,'hetero-cluster_2','hetero-cluster_2', 7);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(23,'hetero-cluster_fra_1','hetero-cluster_fra_1', 7);


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (31,1,7,1,0,'jq',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (32,2,7,1,0,'oy',1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (33,3,7,1,0,'fra',0);

--jq
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (51,31,21,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (52,31,22,1,1);
--oy
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (53,32,21,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (54,32,22,2,1);
--fra
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (55,33,23,3,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(61,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(62,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(81,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.15',6020,'keeper',0,0,15,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(82,'ffffffffffffffffffffffffffffffffffffffff',51,'127.0.1.17',6021,'keeper',0,0,17);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(63,'unknown',51,'127.0.1.1',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(64,'unknown',51,'127.0.1.1',6479,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(65,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.1',6020,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(66,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.1',6021,'keeper',0,0,2);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(83,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.16',6020,'keeper',0,0,16,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(84,'ffffffffffffffffffffffffffffffffffffffff',52,'127.0.2.18',6021,'keeper',0,0,18);
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
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(3, 22, 2, '127.0.0.1', 16001, 1, 1);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(4, 22, 2, '127.0.0.2', 16001, 0, 2);

insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(1, 7, 1, 1, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(2, 7, 1, 1, 3);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type) values (8,'hetero-cluster2',2,'Cluster:Hetero , ActiveDC : A','0000000000000000','Normal',1, 0,'', 'hetero');

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(24,'hetero-cluste1r_1','hetero-cluster_1', 8);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(25,'hetero-cluster1_2','hetero-cluster_2', 8);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(26,'hetero-cluster_fra1_1','hetero-cluster_fra_1', 8);

insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(5, 24, 4, '127.0.0.3', 16000, 1, 3);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(6, 24, 4, '127.0.0.4', 16000, 0, 4);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(7, 25, 4, '127.0.0.1', 16003, 1, 1);
insert into APPLIER_TBL(id, shard_id, repl_direction_id, ip, port, active, container_id) values(8, 25, 4, '127.0.0.2', 16003, 0, 2);


insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(3, 8, 1, 1, 2);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(4, 8, 1, 1, 3);
insert into REPL_DIRECTION_TBL (id, cluster_id,src_dc_id,from_dc_id,to_dc_id) values(5, 8, 2, 2, 3);