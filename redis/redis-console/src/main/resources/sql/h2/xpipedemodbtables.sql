-- Xpipe DB Demo

-- DC Table
drop table if exists DC_TBL;
create table DC_TBL
(
	id bigint unsigned not null auto_increment primary key ,
	dc_name varchar(128) not null unique, 
	dc_active tinyint(1) not null default 1,
	dc_description varchar(1024) not null default 'nothing',
    dc_last_modified_time varchar(40) not null default '',
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);


-- Meta Server Table
drop table if exists METASERVER_TBL;
create table METASERVER_TBL
(
	id bigint unsigned not null auto_increment primary key,
	metaserver_name varchar(128) not null unique ,
	dc_id bigint unsigned not null ,
	metaserver_ip varchar(40) not null,
	metaserver_port int not null ,
	metaserver_active tinyint(1) default 1 not null,
	metaserver_role varchar(12) not null default 'slave',
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0 
);

-- Setinel Table
drop table if exists SETINEL_TBL;
create table SETINEL_TBL
(
	setinel_id bigint unsigned not null auto_increment primary key,
    dc_id bigint unsigned not null,
    setinel_address varchar(128) not null,
    setinel_description varchar(1024) not null default 'nothing',
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);

-- Cluster Table
drop table if exists CLUSTER_TBL;
create table CLUSTER_TBL
(
	id bigint unsigned not null auto_increment primary key,
	cluster_name varchar(128) not null unique,
	activedc_id bigint unsigned not null,
	cluster_description varchar(1024) not null default 'nothing',
    cluster_last_modified_time varchar(40) not null default '',
    status varchar(24) not null default 'normal',
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0,
	is_xpipe_interested tinyint(1) default 0,
	cluster_org_id bigint unsigned not null default 0,
    cluster_admin_emails varchar(250) default ' '
);


-- DC Cluster Table
drop table if exists DC_CLUSTER_TBL;
create table DC_CLUSTER_TBL 
(
	dc_cluster_id bigint unsigned not null auto_increment primary key,
	dc_id bigint unsigned not null,
	cluster_id bigint unsigned not null,
	metaserver_id bigint unsigned not null,
    dc_cluster_phase int not null default 1,
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);

-- Shard Table
drop table if exists SHARD_TBL;
create table SHARD_TBL
(
	id bigint unsigned not null auto_increment primary key,
	shard_name varchar(128) not null,
	cluster_id bigint unsigned not null,
    setinel_monitor_name varchar(128) not null default 'default',
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);


-- DC Cluster Shard Table
drop table if exists DC_CLUSTER_SHARD_TBL;
create table DC_CLUSTER_SHARD_TBL
(
	dc_cluster_shard_id bigint unsigned not null auto_increment primary key,
	dc_cluster_id bigint not null,
	shard_id bigint unsigned not null,
    setinel_id bigint unsigned  not null,
    dc_cluster_shard_phase int not null default 1,
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);


-- Redis Table
drop table if exists REDIS_TBL;
create table REDIS_TBL
(
	id bigint unsigned not null auto_increment primary key,
	run_id varchar(128) not null,
	dc_cluster_shard_id bigint not null,
	redis_ip varchar(40) not null,
	redis_port int not null,
	redis_role varchar(12) not null default 'redis',
	keeper_active tinyint(1) not null default 0,
	master tinyint(1) not null default 0,
	redis_master bigint unsigned default null,
	keepercontainer_id bigint unsigned default null,
   	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);


-- Keeper Container Table
drop table if exists KEEPERCONTAINER_TBL;
create table KEEPERCONTAINER_TBL
(
	keepercontainer_id bigint unsigned not null auto_increment primary key,
    keepercontainer_dc bigint unsigned not null,
	keepercontainer_ip varchar(40) not null,
	keepercontainer_port int not null,
	keepercontainer_active tinyint(1) not null default 1,
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0,
	keepercontainer_org_id bigint(20) unsigned NOT NULL DEFAULT 0
);

-- Migration Event Table
drop table if exists MIGRATION_EVENT_TBL;
create table MIGRATION_EVENT_TBL
(
	id bigint unsigned not null auto_increment primary key,
	start_time timestamp default CURRENT_TIMESTAMP,
	operator varchar(128) not null default 'xpipe',
	event_tag varchar(150) not null,
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);

-- Migration Cluster Table
drop table if exists MIGRATION_CLUSTER_TBL;
create table MIGRATION_CLUSTER_TBL
(
	id bigint unsigned not null auto_increment primary key,
	migration_event_id bigint unsigned not null default 0,
	cluster_id bigint unsigned not null default 0,
	source_dc_id bigint unsigned not null default 0,
	destination_dc_id bigint unsigned not null default 0,
	start_time timestamp default CURRENT_TIMESTAMP,
	end_time timestamp null default null,
	status varchar(16) not null default 'initiated',
	publish_info varchar(10240) not null default '',
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);

-- Migration Shard Table 
drop table if exists MIGRATION_SHARD_TBL;
create table MIGRATION_SHARD_TBL
(
	id bigint unsigned not null auto_increment primary key,
	migration_cluster_id bigint unsigned not null default 0,
	shard_id bigint unsigned not null default 0,
	log varchar(20480) not null default '',
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);


-- Config Table
drop table if exists config_tbl;
CREATE TABLE `config_tbl` (
  id bigint unsigned NOT NULL AUTO_INCREMENT primary key,
  key varchar(128) NOT NULL DEFAULT '' unique,
  value varchar(1024) DEFAULT '' ,
  desc varchar(1024) NOT NULL DEFAULT '' ,
  DataChange_LastTime timestamp DEFAULT CURRENT_TIMESTAMP,
  deleted tinyint(4) NOT NULL DEFAULT 0,
);
INSERT INTO config_tbl (`key`, `value`, `desc`) VALUES ('sentinel.auto.process', 'true', '自动增删哨兵');

-- Organization Table
drop table if exists organization_tbl;
CREATE TABLE `organization_tbl` (
  id bigint(20) unsigned not null AUTO_INCREMENT primary key,
  org_id  bigint(20) unsigned not null default 0 unique,
  org_name varchar(250) not null default 'none' unique,
  DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
  deleted tinyint(4) not null default 0,
);
INSERT INTO organization_tbl (`org_id`, `org_name`) VALUES ('0', '');
