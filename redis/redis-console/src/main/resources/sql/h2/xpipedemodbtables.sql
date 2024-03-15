-- Xpipe DB Demo

-- ZONE Table
drop table if exists ZONE_TBL;
create table ZONE_TBL
(
	id bigint unsigned not null auto_increment primary key ,
	zone_name varchar(128) not null unique,
	DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0
);

-- AZ GROUP TABLE
drop table if exists AZ_GROUP_TBL;
CREATE TABLE `AZ_GROUP_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `name` varchar(20) NOT NULL DEFAULT '' COMMENT 'az group name',
  `region` varchar(20) NOT NULL DEFAULT '' COMMENT 'az group region',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `DataChange_LastTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'last modify time',
  `deleted` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT 'logic delete',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name_deleted` (`name`,`deleted`)
);

-- DC Table
drop table if exists DC_TBL;
CREATE TABLE `DC_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `zone_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'zone id',
  `dc_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'dc name',
  `dc_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'dc active status',
  `dc_description` varchar(1024) NOT NULL DEFAULT 'nothing' COMMENT 'dc description',
  `dc_last_modified_time` varchar(40) NOT NULL DEFAULT '' COMMENT 'last modified tag',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`,`deleted`),
  UNIQUE KEY `dc_name` (`dc_name`,`deleted`)
);


-- AZ GROUP MAPPING Table
drop table if exists AZ_GROUP_MAPPING_TBL;
CREATE TABLE `AZ_GROUP_MAPPING_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `az_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id now',
  `az_group_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference az group id',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `DataChange_LastTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'last modify time',
  `deleted` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT 'logic delete',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_az_group` (`az_id`,`az_group_id`)
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
create table CLUSTER_TBL (
	id bigint unsigned not null auto_increment primary key,
	cluster_name varchar(128) not null unique,
    cluster_type varchar(32) not null default 'ONE_WAY',
	activedc_id bigint unsigned not null default 0,
	cluster_description varchar(1024) not null default 'nothing',
	cluster_org_id bigint unsigned not null default 0,
	cluster_admin_emails varchar(250) default '',
	status varchar(24) not null default 'Normal',
    migration_event_id bigint unsigned not null default 0 COMMENT 'related migration event on processing',
    is_xpipe_interested tinyint(1) default 1,
    cluster_designated_route_ids varchar(1024) not null default '',
    create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'cluster create time',
    cluster_last_modified_time varchar(40) not null default '20230101000000000',
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0,
);


-- AZ GROUP CLUSTER TABLE
drop table if exists AZ_GROUP_CLUSTER_TBL;
CREATE TABLE `AZ_GROUP_CLUSTER_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference cluster id',
  `az_group_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference az_group id',
  `active_az_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'active ac id in az group',
  `az_group_cluster_type` varchar(32) NOT NULL DEFAULT '' COMMENT 'cluster type of az group',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `DataChange_LastTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'last modify time',
  `deleted` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT 'logic delete',
  `delete_time` timestamp NOT NULL DEFAULT '2003-12-09 09:30:00' COMMENT 'logic delete time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_cluster_az_group` (`cluster_id`,`az_group_id`)
);

-- DC Cluster Table
drop table if exists DC_CLUSTER_TBL;
CREATE TABLE `DC_CLUSTER_TBL` (
  `dc_cluster_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference cluster id',
  `az_group_cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference az group cluster id',
  `metaserver_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference metaserver id',
  `dc_cluster_phase` int(11) NOT NULL DEFAULT '1' COMMENT 'dc cluster phase',
  `active_redis_check_rules` varchar(128) DEFAULT NULL COMMENT 'active redis check rules',
  `group_name` varchar(20) DEFAULT NULL COMMENT 'reference group name, null means same as dc name',
  `group_type` varchar(32) DEFAULT NULL COMMENT 'reference group type, DRMaster and Master',
  `DataChange_LastTime` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`dc_cluster_id`)
);

-- Shard Table
drop table if exists SHARD_TBL;
create table SHARD_TBL
(
	id bigint unsigned not null auto_increment primary key,
	shard_name varchar(128) not null,
	cluster_id bigint unsigned not null,
	az_group_cluster_id bigint unsigned not null default 0,
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
	deleted tinyint(1) not null default 0,
    deleted_at int not null default 0,
    UNIQUE KEY `ip_port_deleted_at` (`redis_ip`,`redis_port`, `deleted_at`)
);


-- Keeper Container Table
drop table if exists KEEPERCONTAINER_TBL;
create table KEEPERCONTAINER_TBL
(
	keepercontainer_id bigint unsigned not null auto_increment primary key,
    keepercontainer_dc bigint unsigned not null,
    az_id bigint(20) unsigned NOT NULL DEFAULT 0,
	keepercontainer_ip varchar(40) not null,
	keepercontainer_port int not null,
	keepercontainer_active tinyint(1) not null default 1,
    DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
	deleted tinyint(1) not null default 0,
	keepercontainer_org_id bigint(20) unsigned NOT NULL DEFAULT 0,
    keepercontainer_disk_type varchar(64) not null default 'default'
);

-- Migration Event Table
drop table if exists MIGRATION_EVENT_TBL;
create table MIGRATION_EVENT_TBL
(
	id bigint unsigned not null auto_increment primary key,
	start_time timestamp default CURRENT_TIMESTAMP,
	break tinyint(1) not null default 0 COMMENT 'break or not',
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
  key varchar(128) NOT NULL DEFAULT '',
  sub_key varchar(128) NOT NULL DEFAULT '',
  value varchar(1024) DEFAULT '' ,
  until TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  latest_update_user varchar(512) DEFAULT '',
  latest_update_ip varchar(128) DEFAULT '',
  desc varchar(1024) NOT NULL DEFAULT '' ,
  DataChange_LastTime timestamp DEFAULT CURRENT_TIMESTAMP,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  UNIQUE KEY `key_sub_key` (`key`,`sub_key`)
);
INSERT INTO config_tbl (`key`, `value`, `desc`) VALUES ('sentinel.auto.process', 'true', '自动增删哨兵');
INSERT INTO config_tbl (`key`, `value`, `desc`) VALUES ('alert.system.on', 'true', '邮件报警系统开关');

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

-- Event Table
drop table if exists event_tbl;
CREATE TABLE EVENT_TBL (
  `id` bigint unsigned not null AUTO_INCREMENT primary key,
  `event_type`  varchar(20) not null default 'none',
  `event_operator` varchar(128) not null default  'none',
  `event_operation` varchar(120) not null default  'none',
  `event_detail` varchar(512) not null default  'none',
  `event_property` varchar(512) not null default  'none',
  `DataChange_LastTime` timestamp default CURRENT_TIMESTAMP,
  `deleted` tinyint(4) not null default 0,
);

-- Route Table
drop table if exists route_tbl;
CREATE TABLE `route_tbl` (
  `id` bigint unsigned not null AUTO_INCREMENT primary key,
  `route_org_id` bigint(20) unsigned not null default 0,
  `src_dc_id` bigint(20) unsigned not null,
  `dst_dc_id` bigint(20) unsigned not null,
  `src_proxy_ids` varchar(128) not null default '',
  `dst_proxy_ids` varchar(128) not null default '',
  `optional_proxy_ids` varchar(128) not null default '',
  `is_public` tinyint(1) not null default 1,
  `active` tinyint(1) not null default 1,
  `tag` varchar(128) not null default 'META',
  `description` varchar(1024) NOT NULL DEFAULT '',
  `DataChange_LastTime` timestamp default CURRENT_TIMESTAMP,
  `deleted` tinyint(4) not null default 0,
);

-- Proxy Table
drop table if exists proxy_tbl;
CREATE TABLE `proxy_tbl` (
  `id` bigint unsigned  not null AUTO_INCREMENT primary key,
  `dc_id` bigint(20) unsigned not null default 0,
  `uri` varchar(128) not null default '',
  `active` tinyint(1) not null default 1,
  `monitor_active` tinyint(1) not null default 0,
  `DataChange_LastTime` timestamp default CURRENT_TIMESTAMP,
  `deleted` tinyint(4) not null default 0,
);

-- az_tbl
drop table if exists az_tbl;
CREATE TABLE `az_tbl` (
  `id` bigint(20) unsigned not null AUTO_INCREMENT primary key,
  `dc_id` bigint(20) unsigned not null default 0,
  `active` tinyint(1) not null default 0 ,
  `az_name` varchar(128) not null default '',
  `description` varchar(1024) not null default '',
  `DataChange_LastTime` timestamp default CURRENT_TIMESTAMP,
  `deleted` tinyint(1) not null default 0,
) ;

-- redis_check_rule_tbl
drop table if exists redis_check_rule_tbl;
CREATE TABLE `redis_check_rule_tbl` (
  `id` bigint(20) unsigned not null AUTO_INCREMENT primary key,
  `check_type` varchar(128) not null default '',
  `param` varchar(256) not null default '',
  `description` varchar(1024) not null default '',
  `DataChange_LastTime` timestamp default CURRENT_TIMESTAMP,
  `deleted` tinyint(1) not null default 0,
);

-- sentinel_group_tbl
drop table if exists sentinel_group_tbl;
CREATE TABLE `sentinel_group_tbl`
(
    `sentinel_group_id`   bigint(20) NOT NULL AUTO_INCREMENT primary key,
    `cluster_type`        varchar(40)  NOT NULL DEFAULT '',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0',
    `active`             tinyint(4) NOT NULL DEFAULT '1',
    `datachange_lasttime` timestamp default CURRENT_TIMESTAMP,
    `sentinel_description` varchar(100)  NOT NULL DEFAULT '',
) ;

-- sentinel_tbl
drop table if exists sentinel_tbl;
CREATE TABLE `sentinel_tbl`
(
    `sentinel_id`         bigint(20) NOT NULL AUTO_INCREMENT primary key,
    `dc_id`               bigint(20) NOT NULL DEFAULT '0',
    `sentinel_group_id`   bigint(20) NOT NULL DEFAULT '0',
    `sentinel_ip`         varchar(40)  NOT NULL DEFAULT '0.0.0.0',
    `sentinel_port`       int(11) NOT NULL DEFAULT '0',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0',
    `datachange_lasttime` timestamp default CURRENT_TIMESTAMP,
) ;


-- repl_direction_tbl
drop table if exists repl_direction_tbl;
CREATE TABLE `repl_direction_tbl` (
  id bigint(20) NOT NULL AUTO_INCREMENT primary key,
  cluster_id bigint(20) NOT NULL DEFAULT 0,
  src_dc_id bigint(20) NOT NULL DEFAULT 0,
  from_dc_id bigint(20) NOT NULL DEFAULT 0,
  to_dc_id bigint(20) NOT NULL DEFAULT 0,
  target_cluster_name varchar(128) DEFAULT NULL,
  DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
  deleted tinyint(1) NOT NULL DEFAULT 0,
) ;

-- appliercontainer_tbl
drop table if exists appliercontainer_tbl;
CREATE TABLE `appliercontainer_tbl` (
  appliercontainer_id bigint(20) NOT NULL AUTO_INCREMENT primary key,
  appliercontainer_dc bigint(20)  NOT NULL DEFAULT 0,
  appliercontainer_az bigint(20)  NOT NULL DEFAULT 0,
  appliercontainer_ip varchar(40) NOT NULL DEFAULT '0.0.0.0',
  appliercontainer_port int(11) NOT NULL DEFAULT 0,
  appliercontainer_active tinyint(1) NOT NULL DEFAULT 1,
  appliercontainer_org bigint(20)  NOT NULL DEFAULT 0,
  DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
  deleted tinyint(1) NOT NULL DEFAULT 0,
);

-- applier_tbl
drop table if exists applier_tbl;
CREATE TABLE `applier_tbl` (
  id bigint(20) NOT NULL AUTO_INCREMENT primary key,
  shard_id bigint(20) NOT NULL DEFAULT 0,
  repl_direction_id bigint(20) NOT NULL DEFAULT 0,
  ip varchar(40) NOT NULL DEFAULT '0.0.0.0',
  port int(11) NOT NULL DEFAULT 0,
  active tinyint(1) NOT NULL DEFAULT 0,
  container_id bigint(20) NOT NULL DEFAULT 0,
  DataChange_LastTime timestamp default CURRENT_TIMESTAMP,
  deleted tinyint(1) NOT NULL DEFAULT 0,
  deleted_at int NOT NULL DEFAULT 0,
);
