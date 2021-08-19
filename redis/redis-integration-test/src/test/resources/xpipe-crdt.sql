
insert into CONFIG_TBL (`key`, sub_key, `value`, `desc`, `deleted`) VALUES ('LEASE', 'CROSS_DC_LEADER', 'jq', 'lease for cross dc leader', 0);
insert into organization_tbl(org_id, org_name) values (1, 'org-1');

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_type)
values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : jq','0000000000000000','Normal',1, 0, 'bi_direction');


insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (1,1,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (2,2,1,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (3,3,1,1,0);



insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(1,'shard1','shard1', 1);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (1,1,1,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (2,2,1,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (3,3,1,3,1);

-- redis
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(1,'unknown',1,'127.0.0.1',36379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(2,'unknown',1,'127.0.0.1',36380,'redis',0,1,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(3,'unknown',2,'127.0.0.1',37379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(4,'unknown',2,'127.0.0.1',37380,'redis',0,3,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(5,'unknown',3,'127.0.0.1',38379,'redis',1,0,null);
insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) values(6,'unknown',3,'127.0.0.1',38380,'redis',0,5,null);
--proxy
insert into proxy_tbl (id,dc_id,uri,monitor_active) values(0, 1,'PROXYTCP://127.0.0.1:11080',1);
insert into proxy_tbl (id,dc_id,uri,monitor_active) values(1, 1, 'PROXYTLS://127.0.0.1:11443',1);

insert into proxy_tbl (id,dc_id,uri,monitor_active) values(2, 3,'PROXYTCP://127.0.0.1:11081',1);
insert into proxy_tbl (id,dc_id,uri,monitor_active) values(3, 3,'PROXYTLS://127.0.0.1:11444',1);

insert into proxy_tbl (id,dc_id,uri) values(4, 1,'PROXYTCP://127.0.0.1:11082');
insert into proxy_tbl (id,dc_id,uri) values(5, 1,'PROXYTLS://127.0.0.1:11445');

insert into proxy_tbl (id,dc_id,uri) values(6, 3,'PROXYTCP://127.0.0.1:11083');
insert into proxy_tbl (id,dc_id,uri) values(7, 3,'PROXYTLS://127.0.0.1:11446');

--backup


-- route

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted) values (1, 0, 3, 1, '0,4', '3,7', 1, 'META', 0);
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted) values (2, 0, 1, 3, '2,6', '1,5', 1, 'META', 0);
