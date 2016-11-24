-- Xpipe DB Demo

-- DC Table
drop table if exists DC_TBL;
CREATE TABLE `DC_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'dc name',
  `dc_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'dc active status',
  `dc_description` varchar(1024) NOT NULL DEFAULT 'nothing' COMMENT 'dc description',
  `dc_last_modified_time` varchar(40) NOT NULL DEFAULT '' COMMENT 'last modified tag',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dc_name` (`dc_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) DEFAULT CHARSET=utf8 COMMENT='dc base info';



-- Meta Server Table
drop table if exists METASERVER_TBL;
CREATE TABLE `METASERVER_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `metaserver_name` varchar(128) NOT NULL DEFAULT 'default' COMMENT 'metaserver name',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id',
  `metaserver_ip` varchar(40) NOT NULL DEFAULT '0.0.0.0' COMMENT 'metaserver ip',
  `metaserver_port` int(11) NOT NULL DEFAULT '0' COMMENT 'metaserver port',
  `metaserver_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'metaserver active status',
  `metaserver_role` varchar(12) NOT NULL DEFAULT 'slave' COMMENT 'metaserver role',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  UNIQUE KEY `metaserver_name` (`metaserver_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `dc_id` (`dc_id`)
) DEFAULT CHARSET=utf8 COMMENT='metaserver base info';


-- Setinel Table
drop table if exists SETINEL_TBL;
CREATE TABLE `SETINEL_TBL` (
  `setinel_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'setinel id',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id',
  `setinel_address` varchar(128) NOT NULL DEFAULT 'default' COMMENT 'setinel address',
  `setinel_description` varchar(1024) NOT NULL DEFAULT 'nothing' COMMENT 'setinel description',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`setinel_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `dc_id` (`dc_id`)
) DEFAULT CHARSET=utf8 COMMENT='setinel base info';


-- Cluster Table
drop table if exists CLUSTER_TBL;
CREATE TABLE `CLUSTER_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `cluster_name` varchar(128) NOT NULL DEFAULT 'default' COMMENT 'cluster name',
  `activedc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'active dc id',
  `cluster_description` varchar(1024) NOT NULL DEFAULT 'nothing' COMMENT 'cluster description',
  `cluster_last_modified_time` varchar(40) NOT NULL DEFAULT '' COMMENT 'last modified tag',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  `is_xpipe_interested` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'is xpipe interested',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name` (`cluster_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `is_xpipe_interested` (`is_xpipe_interested`)
) DEFAULT CHARSET=utf8 COMMENT='clusters info';


-- DC Cluster Table
drop table if exists DC_CLUSTER_TBL;
CREATE TABLE `DC_CLUSTER_TBL` (
  `dc_cluster_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference cluster id',
  `metaserver_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference metaserver id',
  `dc_cluster_phase` int(11) NOT NULL DEFAULT '1' COMMENT 'dc cluster phase',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`dc_cluster_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `DcId` (`dc_id`),
  KEY `ClusterId` (`cluster_id`)
) DEFAULT CHARSET=utf8 COMMENT='dc cluster base info';


-- Shard Table
drop table if exists SHARD_TBL;
CREATE TABLE `SHARD_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `shard_name` varchar(128) NOT NULL DEFAULT 'default' COMMENT 'shard name',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference cluster id',
  `setinel_monitor_name` varchar(128) NOT NULL DEFAULT 'default' COMMENT 'setinel monitor name',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChangeLastTime` (`DataChange_LastTime`),
  KEY `cluster_id` (`cluster_id`)
) DEFAULT CHARSET=utf8 COMMENT='shard base info';



-- DC Cluster Shard Table
drop table if exists DC_CLUSTER_SHARD_TBL;
CREATE TABLE `DC_CLUSTER_SHARD_TBL` (
  `dc_cluster_shard_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_cluster_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'reference dc cluster id',
  `shard_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference shard id',
  `setinel_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'setinel id',
  `dc_cluster_shard_phase` int(11) NOT NULL DEFAULT '1' COMMENT 'structure phase',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`dc_cluster_shard_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `DcClusterId` (`dc_cluster_id`),
  KEY `ShardId` (`shard_id`)
) DEFAULT CHARSET=utf8 COMMENT='dc cluster shard base info';



-- Redis Table
drop table if exists REDIS_TBL;
CREATE TABLE `REDIS_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `run_id` varchar(128) NOT NULL DEFAULT 'unknown' COMMENT 'redis runid',
  `dc_cluster_shard_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'reference dc cluster shard id',
  `redis_ip` varchar(40) NOT NULL DEFAULT '0.0.0.0' COMMENT 'redis ip',
  `redis_port` int(11) NOT NULL DEFAULT '0' COMMENT 'redis port',
  `redis_role` varchar(12) NOT NULL DEFAULT 'redis' COMMENT 'redis role',
  `keeper_active` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'redis active status',
  `master` tinyint(1) DEFAULT '0' COMMENT 'redis master tag',
  `redis_master` bigint(20) unsigned DEFAULT NULL COMMENT 'redis master id',
  `keepercontainer_id` bigint(20) unsigned DEFAULT NULL COMMENT 'keepercontainer id',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `DcClusterShardId` (`dc_cluster_shard_id`),
  KEY `keeper_active` (`keeper_active`)
) DEFAULT CHARSET=utf8 COMMENT='redis base info';



-- Keeper Container Table
drop table if exists KEEPERCONTAINER_TBL;
CREATE TABLE `KEEPERCONTAINER_TBL` (
  `keepercontainer_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `keepercontainer_dc` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference keepercontainer dc',
  `keepercontainer_ip` varchar(40) NOT NULL DEFAULT '0.0.0.0' COMMENT 'keepercontainer ip',
  `keepercontainer_port` int(11) NOT NULL DEFAULT '0' COMMENT 'keepercontainer port',
  `keepercontainer_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'keepercontainer active status',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`keepercontainer_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `keepercontainer_dc` (`keepercontainer_dc`)
) DEFAULT CHARSET=utf8 COMMENT='keepercontainer base info';
