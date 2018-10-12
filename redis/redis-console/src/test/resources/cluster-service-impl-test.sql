insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (1,'cluster1',1,'first',0,1,'first@ctrip.com');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (2,'cluster2',1,'second',1,1,'second@ctrip.com');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (3,'cluster3',1,'third',0,2,'third@ctrip.com');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (4,'cluster4',2,'four',0,2,'four@ctrip.com');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (5,'cluster5',3,'five',0,3,'five@ctrip.com');
insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,deleted,cluster_org_id,cluster_admin_emails) values (6,'cluster6',2,'six',1,2,'six@ctrip.com');

insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (1,1,1,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (2,1,2,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (3,2,1,1,0,1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (4,3,5,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (5,2,1,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (6,1,6,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (7,3,4,1,0,1);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (8,1,3,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (9,2,6,1,0,0);
insert into DC_CLUSTER_TBL (dc_cluster_id,dc_id,cluster_id,dc_cluster_phase,metaserver_id,deleted) values (10,2,4,1,0,0);

insert into organization_tbl(org_id, org_name,deleted) values (1, 'org-1',0), (2, 'org-2',1), (3, 'org-3',0);

