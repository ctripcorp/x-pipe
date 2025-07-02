insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (1,1,1,1,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (2,3,2,1,0,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (3,2,2,3,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (4,3,2,4,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (5,1,3,0,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (6,2,4,0,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (7,1,5,0,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (8,2,6,0,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (9,1,7,0,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (10,2,8,0,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (11,1,9,5,1,0,'jq','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (12,2,9,6,1,0,'oy','DR_MASTER');
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,az_group_cluster_id,dc_cluster_phase,metaserver_id, group_name, group_type) values (13,3,9,7,1,0,'fra','DR_MASTER');

-- 3个org
insert into organization_tbl(org_id, org_name,deleted) values (1, 'org-1',0), (2, 'org-2',1), (3, 'org-3',0);

-- cluster
-- 异构集群1 activedc jq
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (1,'hetero-cluster1',1,'Cluster:Hetero , ActiveDC : jq','0000000000000000','Normal',1, 1,'', 'hetero');

-- 异构集群2 activedc oy
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (2,'hetero-cluster2',2,'Cluster:Hetero , ActiveDC : oy','0000000000000000','Normal',1, 2,'', 'hetero');

-- 单向同步集群1 activedc jq
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (3,'one-cluster1',1,'Cluster:one_way , ActiveDC : jq','0000000000000000','Normal',1, 1,'', 'one_way');

-- 单向同步集群2 activedc oy
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (4,'one-cluster2',2,'Cluster:one_way , ActiveDC : oy','0000000000000000','Normal',1, 2,'', 'one_way');

-- 单机房集群1 activedc jq
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (5,'single-cluster1',1,'Cluster:single_dc , ActiveDC : jq','0000000000000000','Normal',1, 1,'', 'single_dc');

-- 单机房集群2 activedc oy
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (6,'single-cluster2',2,'Cluster:single_dc , ActiveDC : oy','0000000000000000','Normal',1, 2,'', 'single_dc');

-- 其他集群1 activedc jq
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (7,'cross-cluster1',1,'Cluster:cross_dc , ActiveDC : jq','0000000000000000','Normal',1, 1,'', 'cross_dc');

-- 其他集群2 activedc oy
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (8,'local-cluster2',2,'Cluster:local_dc , ActiveDC : oy','0000000000000000','Normal',1, 2,'', 'local_dc');

-- 异构集群3 activedc jq
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id, cluster_designated_route_ids, cluster_type)
values (9,'hetero-cluster3',1,'Cluster:Hetero , ActiveDC : jq','0000000000000000','Normal',1, 2,'', 'hetero');

-- az_group_cluster
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (1,1,3,1,'ONE_WAY');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (2,1,2,3, 'SINGLE_DC');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (3,2,4,2,'ONE_WAY');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (4,2,2,3,'SINGLE_DC');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (5,9,3,1,'ONE_WAY');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (6,9,4,2,'SINGLE_DC');
insert into AZ_GROUP_CLUSTER_TBL (id,cluster_id,az_group_id,active_az_id,az_group_cluster_type) values (7,9,2,3,'SINGLE_DC');


