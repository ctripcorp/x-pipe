insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (101, 101,101,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (102, 101,101,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (103, 103,101,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (104, 103,101,1,1);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (101,1,101,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (102,2,101,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (103,1,102,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (104,2,102,1,0);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(101,'ffffffffffffffffffffffffffffffffffffffff',101,'127.0.0.1',6600,'keeper',0,-1,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(102,'ffffffffffffffffffffffffffffffffffffffff',102,'127.0.0.2',6601,'keeper',0,-1,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(103,'ffffffffffffffffffffffffffffffffffffffff',103,'127.0.0.1',6700,'keeper',0,-1,4,1);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, keeper_active) values(104,'ffffffffffffffffffffffffffffffffffffffff',104,'127.0.0.2',6701,'keeper',0,-1,4,1);

insert into organization_tbl(org_id, org_name) values (1, 'org-1');

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (101,'cluster101',1,'Cluster:cluster101 , ActiveDC : A','0000000000000000','Normal',1, 2);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (102,'cluster102',1,'Cluster:cluster102 , ActiveDC : A','0000000000000000','Normal',1, 2);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(101,'cluster101_shard1','cluster101_shard1', 101);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(102,'cluster101_shard2','cluster101_shard2', 101);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_type, cluster_designated_route_ids) values (104,'bi-cluster1',0,'bi-cluster:cluster1 , ActiveDC : B','0000000000000000','Normal',1, 2, 'BI_DIRECTION', '');

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,active_redis_check_rules) values (8,1,104,1,0,'1,2');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,active_redis_check_rules) values (9,2,104,1,0,'1,2');
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (1,1,'PROXYTCP://172.19.0.20:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (2,2,'PROXYTLS://172.19.0.21:443',1,1,0);

insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (3,1,'PROXYTCP://172.19.0.22:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (4,2,'PROXYTLS://172.19.0.23:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (20,2,'PROXYTLS://172.19.0.90:443',1,1,0);

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (1, 0, 1, 2, '1', '2', 1, 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (2, 0, 1, 2, '3', '20', '4', 1, 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (3, 1, 2, 1, '5', '6', 1, 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (4, 1, 2, 1, '7', '8', 1, 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (5, 0, 2, 1, '9', '10', 1, 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, is_public, tag, deleted, description) values (6, 0, 2, 1, '11', '12', 1, 1, 'meta', 0, 'desc');

