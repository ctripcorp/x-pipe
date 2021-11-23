insert into AZ_TBL (id, dc_id, az_name, active, description) values (1, 3, 'A', 1, 'zone for dc:fra zone A');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (2, 3, 'B', 1, 'zone for dc:fra zone B');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (3, 3, 'C', 0, 'zone for dc:fra zone C');

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (30,3,'127.0.1.2',7033,1,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (31,3,'127.1.1.2',7034,1,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (32,3,'127.0.1.2',7035,1,2,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (33,3,'127.0.1.2',7036,1,2,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (34,3,'127.0.1.2',7037,1,3,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (35,3,'127.0.1.2',7038,1,3,0);

insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (6,'cluster6',3,'Cluster:cluster6 , ActiveDC : A','0000000000000000','Normal',1, 0);

insert into SHARD_TBL (id,shard_name,setinel_monitor_name,cluster_id) values(9,'cluster6shard1','cluster6shard1', 6);

insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id, deleted) values(22,'bee',9,'127.1.1.2',7101,'keeper',0,-1,31, 0);

