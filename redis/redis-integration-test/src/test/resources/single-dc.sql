insert into CONFIG_TBL (`key`, sub_key, `value`, `desc`) VALUES ('LEASE', 'CROSS_DC_LEADER', 'jq', 'lease for cross dc leader');

insert into organization_tbl(org_id, org_name) values (1, 'org-1');

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id)
    values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : jq','0000000000000000','Normal',1, 1);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(1,'cluster1-shard1','cluster1-shard1', 1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1,1);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(1,'unknown',1,'127.0.0.1',6379,'redis',1,0,null);
