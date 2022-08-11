insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',1, 1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Normal',1, 2);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (3,'cluster3',1,'Cluster:cluster3 , ActiveDC : A','0000000000000000','Normal',1, 2);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (4,'test_cluster',1,'Cluster:test_cluster , ActiveDC : A','0000000000000000','Normal',1, 2);

insert into MIGRATION_EVENT_TBL (id,event_tag) values (1,'cluster1-1-2');
insert into MIGRATION_EVENT_TBL (id,event_tag) values (2,'cluster2-1-2');
insert into MIGRATION_EVENT_TBL (id,event_tag) values (3,'cluster2-2-3-cluster3-1-3');
insert into MIGRATION_EVENT_TBL (id,event_tag) values (4,'cluster2-3-2-cluster3-3-2');
insert into MIGRATION_EVENT_TBL (id,event_tag) values (5,'test_cluster-1->2');

insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (1,1,1,2,'Success');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (2,2,2,2,'Success');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (3,3,2,3,'Success');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (4,3,3,3,'Success');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (5,4,2,2,'Checking');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (6,4,3,2,'ForceEnd');
insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id, destination_dc_id,status) values (7,5,4,2,'Success');

