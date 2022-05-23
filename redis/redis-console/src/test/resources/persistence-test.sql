insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested) values (1,'Cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',1);
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested) values (2,'Cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Migrating',1);

update config_tbl set until='2020-01-01 00:00:00', `value`='false';