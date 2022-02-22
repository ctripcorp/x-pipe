insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (101,'cluster101',1,'Cluster:cluster101 , ActiveDC : A','0000000000000000','Normal',1, 1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (102,'cluster102',2,'Cluster:cluster102 , ActiveDC : B','0000000000000000','Normal',1, 1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (103,'cluster103',1,'Cluster:cluster103 , ActiveDC : A','0000000000000000','Normal',1, 1);

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (101,1,101,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (102,1,102,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (103,2,101,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (104,2,102,1,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id) values (105,1,103,1,0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(101,'cluster101_1','cluster101_1', 101);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(102,'cluster101_2','cluster101_2', 101);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(103,'cluster102_1','cluster102_1', 102);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(104,'cluster102_2','cluster102_2', 102);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(105,'cluster103_1','cluster103_1', 103);
insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(106,'cluster103_2','cluster103_2', 103);

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (101,101,101,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (102,101,102,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (103,102,103,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (104,102,104,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (105,103,101,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (106,103,102,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (107,104,103,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (108,104,104,2,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (109,105,105,1,1);
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_shard_id,dc_cluster_id,shard_id,setinel_id,dc_cluster_shard_phase) values (110,105,106,1,1);

insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(101,1,'127.0.0.1:1101,127.0.0.2:1101,127.0.0.3:1101','setinel 101');
insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(102,2,'127.0.0.1:1102,127.0.0.2:1102,127.0.0.3:1102','setinel 102');

insert into SENTINEL_GROUP_TBL (sentinel_group_id,cluster_type) values (101,'ONE_WAY');
insert into SENTINEL_GROUP_TBL (sentinel_group_id,cluster_type) values (102,'ONE_WAY');

insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,101,'127.0.0.1',1101);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,101,'127.0.0.2',1101);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (1,101,'127.0.0.3',1101);

insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (2,102,'127.0.0.1',1102);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (2,102,'127.0.0.2',1102);
insert into SENTINEL_TBL (dc_id, sentinel_group_id, sentinel_ip, sentinel_port) values (2,102,'127.0.0.3',1102);
