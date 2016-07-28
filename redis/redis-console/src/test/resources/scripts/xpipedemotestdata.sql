insert into DC_TBL values ('SHAJQ',1,'dc named jq located in shanghai.');
insert into DC_TBL values ('SHAOY',1,'dc named oy located in shanghai.');

insert into METASERVER_TBL values ('JQ_META_1','SHAJQ','127.0.0.1',8808,1,'master');
insert into METASERVER_TBL values ('JQ_META_2','SHAJQ','127.0.0.1',8809,1,'slave');
insert into METASERVER_TBL values ('OY_META_1','SHAOY','0.0.0.1',7777,1,'master');

insert into CLUSTER_TBL values ('JQ_CLUSTER_1','SHA_JQ','Cluster 1 located in JQ');
insert into CLUSTER_TBL values ('OY_CLUSTER_1','SHA_OY','Cluster 1 located in OY');

insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id) values ('SHAJQ', 'JQ_CLUSTER_1','JQ_META_1');
insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id) values ('SHAOY', 'JQ_CLUSTER_1','OY_META_1');
insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id) values ('SHAOY', 'OY_CLUSTER_1','OY_META_1');
insert into DC_CLUSTER_TBL (dc_id,cluster_id,metaserver_id) values ('SHAJQ', 'OY_CLUSTER_1','JQ_META_2'); 


insert into SHARD_TBL values('JQ_CLUSTER_1_SHARD_1','JQ_CLUSTER_1');
insert into SHARD_TBL values('JQ_CLUSTER_1_SHARD_2','JQ_CLUSTER_1');
insert into SHARD_TBL values('OY_CLUSTER_1_SHARD_1','OY_CLUSTER_1');

insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id) values (0,'JQ_CLUSTER_1_SHARD_1');
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id) values (1,'JQ_CLUSTER_1_SHARD_2');
insert into DC_CLUSTER_SHARD_TBL (dc_cluster_id,shard_id) values (2,'OY_CLUSTER_1_SHARD_1');

insert into REDIS_TBL values('JQ_C1_S1_R1',0,'127.0.0.1',9999,'master',1,null,null);
insert into REDIS_TBL values('JQ_C1_S1_R2',0,'127.0.0.1',9998,'slave',1,'JQ_C1_S1_R1',null);
insert into REDIS_TBL values('JQ_C1_S1_R3',0,'127.0.0.1',9997,'keeper',1,'JQ_C1_S1_R1',0);
insert into REDIS_TBL values('JQ_C1_S1_R4',1,'127.0.0.0',9997,'slave',1,'JQ_C1_S1_R3',null);

insert into KEEPERCONTAINER_TBL(keepercontainer_ip,keepercontainer_port,keepercontainer_active) values ('111.111.111.111',2222,1);
