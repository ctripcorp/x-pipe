-- Xpipe DB Demo

-- DC Table
drop table if exists DC_TBL;
create table DC_TBL
(
	id bigint unsigned not null auto_increment primary key comment 'primary key',
	dc_name varchar(128) not null unique comment 'dc name', 
	dc_active tinyint(1) not null default 1 comment 'dc active status',
	dc_description varchar(1024) not null default 'nothing' comment 'dc description',
    	dc_last_modified_time varchar(40) not null default '' comment 'last modified tag',
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);


-- Meta Server Table
drop table if exists METASERVER_TBL;
create table METASERVER_TBL
(
	id bigint unsigned not null auto_increment primary key comment 'primary key',
	metaserver_name varchar(128) not null unique comment 'metaserver name',
	dc_id bigint unsigned not null comment 'reference dc id',
	metaserver_ip varchar(40) not null comment 'metaserver ip',
	metaserver_port int not null comment 'metaserver port',
	metaserver_active tinyint(1) default 1 not null comment 'metaserver active status',
	metaserver_role varchar(12) not null default 'slave' comment 'metaserver role',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);

-- Setinel Table
drop table if exists SETINEL_TBL;
create table SETINEL_TBL
(
	setinel_id bigint unsigned not null auto_increment primary key comment 'setinel id',
    	dc_id bigint unsigned not null comment 'reference dc id',
    	setinel_address varchar(128) not null comment 'setinel address',
    	setinel_description varchar(1024) not null default 'nothing' comment 'setinel description',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);

-- Cluster Table
drop table if exists CLUSTER_TBL;
create table CLUSTER_TBL
(
	id bigint unsigned not null auto_increment primary key comment 'primary key',
	cluster_name varchar(128) not null unique comment 'cluster name',
	activedc_id bigint unsigned not null comment 'active dc id',
	cluster_description varchar(1024) not null default 'nothing' comment 'cluster description',
    	cluster_last_modified_time varchar(40) not null default '' comment 'last modified tag',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);


-- DC Cluster Table
drop table if exists DC_CLUSTER_TBL;
create table DC_CLUSTER_TBL 
(
	dc_cluster_id bigint unsigned not null auto_increment primary key comment 'primary key',
	dc_id bigint unsigned not null comment 'reference dc id',
	cluster_id bigint unsigned not null comment 'reference cluster id',
	metaserver_id bigint unsigned not null comment 'reference metaserver id',
    	dc_cluster_phase int not null default 1 comment 'dc cluster phase',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);

-- Shard Table
drop table if exists SHARD_TBL;
create table SHARD_TBL
(
	id bigint unsigned not null auto_increment primary key comment 'primary key',
	shard_name varchar(128) not null comment 'shard name',
	cluster_id bigint unsigned not null comment 'reference cluster id',
    	setinel_monitor_name varchar(128) not null default 'default' comment 'setinel monitor name',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);


-- DC Cluster Shard Table
drop table if exists DC_CLUSTER_SHARD_TBL;
create table DC_CLUSTER_SHARD_TBL
(
	dc_cluster_shard_id bigint unsigned not null auto_increment primary key comment 'primary key',
	dc_cluster_id bigint not null comment 'reference dc cluster id',
	shard_id bigint unsigned not null comment 'reference shard id',
    	setinel_id bigint unsigned  not null comment 'setinel id',
    	dc_cluster_shard_phase int not null default 1 comment 'structure phase',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);


-- Redis Table
drop table if exists REDIS_TBL;
create table REDIS_TBL
(
	id bigint unsigned not null auto_increment primary key comment 'primary key',
	redis_name varchar(128) not null comment 'redis name',
	dc_cluster_shard_id bigint not null comment 'reference dc cluster shard id',
	redis_ip varchar(40) not null comment 'redis ip',
	redis_port int not null comment 'redis port',
	redis_role varchar(12) not null default 'redis' comment 'redis role',
	keeper_active tinyint(1) not null default 0 comment 'redis active status',
	redis_master bigint unsigned default null comment 'redis master id',
	keepercontainer_id bigint unsigned default null  comment 'keepercontainer id',
   	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);


-- Keeper Container Table
drop table if exists KEEPERCONTAINER_TBL;
create table KEEPERCONTAINER_TBL
(
	keepercontainer_id bigint unsigned not null auto_increment primary key comment 'primary key',
    	keepercontainer_dc bigint unsigned not null comment 'reference keepercontainer dc',
	keepercontainer_ip varchar(40) not null comment 'keepercontainer ip',
	keepercontainer_port int not null comment 'keepercontainer port',
	keepercontainer_active tinyint(1) not null default 1 comment 'keepercontainer active status',
    	DataChange_LastTime timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'last modified time',
	deleted tinyint(1) not null default 0 comment 'deleted or not'
);
