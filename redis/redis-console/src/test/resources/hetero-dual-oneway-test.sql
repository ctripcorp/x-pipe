insert into CLUSTER_TBL(id, cluster_name, activedc_id, cluster_description, cluster_org_id, cluster_type, cluster_admin_emails, cluster_last_modified_time)
values (14, 'hetero-dual-oneway', 1, 'hetero dual one way', 1, 'HETERO', 'test@111.com', 20170426180546626);

insert into AZ_GROUP_CLUSTER_TBL(id, cluster_id, az_group_id, active_az_id, az_group_cluster_type) values(23, 14, 1, 1, 'ONE_WAY');
insert into AZ_GROUP_CLUSTER_TBL(id, cluster_id, az_group_id, active_az_id, az_group_cluster_type) values(24, 14, 2, 3, 'ONE_WAY');

insert into SHARD_TBL(id, shard_name, setinel_monitor_name, cluster_id, az_group_cluster_id) values(38, 'hetero-dual-oneway_jq_1','hetero-dual-oneway_jq_1', 14, 23);
insert into SHARD_TBL(id, shard_name, setinel_monitor_name, cluster_id, az_group_cluster_id) values(39, 'hetero-dual-oneway_fra_1','hetero-dual-oneway_fra_1', 14, 24);

insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (47, 1, 14, 23);
insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (48, 2, 14, 23);
insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (49, 3, 14, 24);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (75,47,38,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (76,48,38,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (77,49,39,3,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(147,'ffffffffffffffffffffffffffffffffffffffff',75,'127.0.0.1',5100,'keeper',0,0,1,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(148,'unknown',75,'10.0.0.11',6379,'redis',1,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(149,'ffffffffffffffffffffffffffffffffffffffff',76,'127.0.0.5',5100,'keeper',0,0,5,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(150,'unknown',76,'10.0.0.12',6379,'redis',0,0,null);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(151,'unknown',77,'10.0.0.13',6379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(152,'unknown',77,'10.0.0.13',6479,'redis',0,0,null);
