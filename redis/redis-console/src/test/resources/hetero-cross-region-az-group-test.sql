-- Cross-region ONE_WAY AzGroup (SHA jq/oy + FRA fra) + overseas SINGLE_DC (SGP)
insert into ZONE_TBL (id, zone_name) values (3, 'SGP');

insert into DC_TBL (id, zone_id, dc_name, dc_active, dc_description, dc_last_modified_time)
values (4, 3, 'sgp', 1, 'DC:SGP', '0000000000000000');

insert into AZ_GROUP_TBL (id, name) values (5, 'CROSS_SHA_FRA');
insert into AZ_GROUP_TBL (id, name) values (6, 'LOCAL_SGP');

insert into AZ_GROUP_MAPPING_TBL (id, az_id, az_group_id) values (6, 1, 5);
insert into AZ_GROUP_MAPPING_TBL (id, az_id, az_group_id) values (7, 2, 5);
insert into AZ_GROUP_MAPPING_TBL (id, az_id, az_group_id) values (8, 3, 5);
insert into AZ_GROUP_MAPPING_TBL (id, az_id, az_group_id) values (9, 4, 6);

insert into SETINEL_TBL (setinel_id, dc_id, setinel_address, setinel_description)
values (4, 4, '127.0.0.1:52222,127.0.0.1:52223', 'setinel sgp');

insert into CLUSTER_TBL(id, cluster_name, activedc_id, cluster_description, cluster_org_id, cluster_type, cluster_admin_emails, cluster_last_modified_time)
values (16, 'hetero-cross-region', 1, 'hetero cross region one way + overseas single dc', 1, 'HETERO', 'test@111.com', 20170426180546626);

insert into AZ_GROUP_CLUSTER_TBL(id, cluster_id, az_group_id, active_az_id, az_group_cluster_type)
values (30, 16, 5, 1, 'ONE_WAY');
insert into AZ_GROUP_CLUSTER_TBL(id, cluster_id, az_group_id, active_az_id, az_group_cluster_type)
values (31, 16, 6, 4, 'SINGLE_DC');

insert into SHARD_TBL(id, shard_name, setinel_monitor_name, cluster_id, az_group_cluster_id)
values (50, 'hetero-cross-region_jq_1', 'hetero-cross-region_jq_1', 16, 30);
insert into SHARD_TBL(id, shard_name, setinel_monitor_name, cluster_id, az_group_cluster_id)
values (51, 'hetero-cross-region_sgp_1', 'hetero-cross-region_sgp_1', 16, 31);

insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (60, 1, 16, 30);
insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (61, 2, 16, 30);
insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (62, 3, 16, 30);
insert into DC_CLUSTER_TBL(dc_cluster_id, dc_id, cluster_id, az_group_cluster_id) values (63, 4, 16, 31);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id, dc_cluster_id, shard_id, setinel_id, dc_cluster_shard_phase)
values (90, 60, 50, 1, 1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id, dc_cluster_id, shard_id, setinel_id, dc_cluster_shard_phase)
values (91, 61, 50, 2, 1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id, dc_cluster_id, shard_id, setinel_id, dc_cluster_shard_phase)
values (92, 62, 50, 3, 1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id, dc_cluster_id, shard_id, setinel_id, dc_cluster_shard_phase)
values (93, 63, 51, 4, 1);

insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id, keeper_active)
values (200, 'ffffffffffffffffffffffffffffffffffffffff', 90, '127.0.0.1', 5200, 'keeper', 0, 0, 1, 1);
insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (201, 'unknown', 90, '10.0.1.11', 6379, 'redis', 1, 0, null);

insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id, keeper_active)
values (202, 'ffffffffffffffffffffffffffffffffffffffff', 91, '127.0.0.5', 5200, 'keeper', 0, 0, 5, 1);
insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (203, 'unknown', 91, '10.0.1.12', 6379, 'redis', 0, 0, null);

insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (204, 'unknown', 92, '10.0.1.13', 6379, 'redis', 0, 0, null);
insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (205, 'unknown', 92, '10.0.1.13', 6479, 'redis', 0, 0, null);

insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (206, 'unknown', 93, '10.0.1.14', 6379, 'redis', 1, 0, null);
insert into REDIS_TBL (id, run_id, dc_cluster_shard_id, redis_ip, redis_port, redis_role, master, redis_master, keepercontainer_id)
values (207, 'unknown', 93, '10.0.1.14', 6479, 'redis', 0, 0, null);
