-- Xpipe DB Demo

-- DC Table
create table DC_TBL
(
	dc_id varchar(30) unique primary key , 
	dc_active tinyint(1) default 1,
	dc_description varchar(180)
);


-- Meta Server Table
create table METASERVER_TBL
(
	metaserver_id varchar(30) not null primary key,
	dc_id varchar(30) not null references DB_TBL(dc_id),
	metaserver_ip varchar(40),
	metaserver_port int,
	metaserver_active tinyint(1) default 1,
	metaserver_role varchar(12)
);


-- Cluster Table
create table CLUSTER_TBL
(
	cluster_id varchar(30) primary key,
	activedc_id varchar(30) references DC_TBL(dc_id),
	cluster_description varchar(180)
);


-- DC Cluster Table
create table DC_CLUSTER_TBL 
(
	dc_cluster_id int auto_increment primary key,
	dc_id varchar(30) references DC_TBL(dc_id),
	cluster_id varchar(30) references CLUSTER_TBL(cluster_id),
	metaserver_id varchar(30) references METASERVER_TBL(metaserver_id)
);

-- Shard Table
create table SHARD_TBL
(
	shard_id varchar(30) not null primary key,
	cluster_id varchar(30) references CLUSTER_TBL(cluster_id)
);


-- DC Cluster Shard Table
create table DC_CLUSTER_SHARD_TBL
(
	dc_cluster_shard_id int auto_increment primary key,
	dc_cluster_id int references DC_CLUSTER_TBL(dc_cluster_id),
	shard_id int references SHARD_TBL(shard_id)
);


-- Redis Table
create table REDIS_TBL
(
	redis_id varchar(80) not null primary key,
	dc_cluster_shard_id int references DC_CLUSTER_SHARD_TBL(dc_cluster_shard_id),
	redis_ip varchar(40),
	redis_port int,
	redis_role varchar(12),
	redis_active tinyint(1) default 1,
	redis_master varchar(80) references REDIS_TBL(redis_id),
	keepercontainer_id int references KEEPERCONTAINER_TBL(keepercontainer_id)
);


-- Keeper Container Table
create table KEEPERCONTAINER_TBL
(
	keepercontainer_id int auto_increment primary key,
	keepercontainer_ip varchar(40),
	keepercontainer_port int,
	keepercontainer_active tinyint(1) default 1
);
