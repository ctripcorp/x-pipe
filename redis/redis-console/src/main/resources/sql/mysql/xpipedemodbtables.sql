-- Xpipe DB Demo

-- ZONE Table
drop table if exists ZONE_TBL;
create table ZONE_TBL
(
	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
	`zone_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'zone name',
	`DataChange_LastTime` timestamp default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
	`deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
	PRIMARY KEY (`id`),
  UNIQUE KEY `zone_name` (`zone_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) DEFAULT CHARSET=utf8 COMMENT='zone base info';

-- DC Table
drop table if exists DC_TBL;
CREATE TABLE `DC_TBL` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `zone_id` bigint(20) unsigned NOT NULL COMMENT 'zone id',
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
  `cluster_type` varchar(32) NOT NULL DEFAULT 'one_way' COMMENT 'cluster type',
  `activedc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'active dc id',
  `cluster_description` varchar(1024) NOT NULL DEFAULT 'nothing' COMMENT 'cluster description',
  `cluster_last_modified_time` varchar(40) NOT NULL DEFAULT '' COMMENT 'last modified tag',
  `status` varchar(24) NOT NULL DEFAULT 'normal' COMMENT 'cluster status',
  `migration_event_id` bigint(20) unsigned NOT Null DEFAULT '0' COMMENT 'related migration event on processing',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  `is_xpipe_interested` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'is xpipe interested',
  `cluster_org_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'organization id of cluster',
  `cluster_admin_emails` varchar(1024) DEFAULT ' ' COMMENT 'persons email who in charge of this cluster',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'cluster create time',

  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name` (`cluster_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `is_xpipe_interested` (`is_xpipe_interested`),
  KEY `Deleted` (`deleted`),
  KEY `Deleted_ClusterType` (`deleted`,`cluster_type`)
) DEFAULT CHARSET=utf8 COMMENT='clusters info';


-- DC Cluster Table
drop table if exists DC_CLUSTER_TBL;
CREATE TABLE `DC_CLUSTER_TBL` (
  `dc_cluster_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference dc id',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference cluster id',
  `metaserver_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference metaserver id',
  `dc_cluster_phase` int(11) NOT NULL DEFAULT '1' COMMENT 'dc cluster phase',
  `redis_config_check_rules` varchar(128) NOT NULL DEFAULT '' COMMENT 'should check rules',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`dc_cluster_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `DcId` (`dc_id`),
  KEY `ClusterId` (`cluster_id`),
  KEY `DcIdPrimary` (`dc_id`,`dc_cluster_id`,`deleted`),
  KEY `ClusterId_Deleted` (`cluster_id`,`deleted`),
  KEY `DcId_Deleted` (`dc_id`,`deleted`)
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
  KEY `ClusterId` (`cluster_id`),
  KEY `Deleted` (`deleted`)
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
  KEY `ShardId` (`shard_id`),
  KEY `ShardId_Deleted` (`shard_id`,`deleted`),
  KEY `Deleted` (`deleted`),
  KEY `DcClusterId_Deleted` (`dc_cluster_id`,`deleted`)
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
  `deleted_at` int(11) NOT NULL DEFAULT '0' COMMENT 'deleted time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ip_port_deleted_at` (`redis_ip`,`redis_port`,`deleted_at`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `DcClusterShardId` (`dc_cluster_shard_id`),
  KEY `keeper_active` (`keeper_active`),
  KEY `DcClusterShardId_Deleted` (`dc_cluster_shard_id`,`deleted`)
) DEFAULT CHARSET=utf8 COMMENT='redis base info';



-- Keeper Container Table
drop table if exists KEEPERCONTAINER_TBL;
CREATE TABLE `KEEPERCONTAINER_TBL` (
  `keepercontainer_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `keepercontainer_dc` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'reference keepercontainer dc',
  `az_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'available zone id',
  `keepercontainer_ip` varchar(40) NOT NULL DEFAULT '0.0.0.0' COMMENT 'keepercontainer ip',
  `keepercontainer_port` int(11) NOT NULL DEFAULT '0' COMMENT 'keepercontainer port',
  `keepercontainer_active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'keepercontainer active status',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  `keepercontainer_org_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'organization id of keeper container',
  PRIMARY KEY (`keepercontainer_id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `keepercontainer_dc` (`keepercontainer_dc`)
) DEFAULT CHARSET=utf8 COMMENT='keepercontainer base info';



-- Migration Event Table
drop table if exists migration_event_tbl;
CREATE TABLE `migration_event_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'event start time',
  `break` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'break or not',
  `operator` varchar(128) NOT NULL DEFAULT 'xpipe' COMMENT 'event operator',
  `event_tag` varchar(150) NOT NULL DEFAULT 'eventtag' COMMENT 'event mark tag',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 COMMENT='migration events table';



-- Migration Cluster Table
drop table if exists migration_cluster_tbl;
CREATE TABLE `migration_cluster_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `migration_event_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'event id according to migration event',
  `cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'cluster id involved in this migration event',
  `source_dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'migration source for this cluster',
  `destination_dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'migration destination for this cluster',
  `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'start time of this migration',
  `end_time` timestamp NULL DEFAULT NULL COMMENT 'end time of this migration',
  `status` varchar(16) NOT NULL DEFAULT 'initiated' COMMENT 'migration status',
  `publish_info` varchar(10240) NOT NULL DEFAULT '' COMMENT 'migration publish information',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `ClusterId_MigrationEventId` (`cluster_id`,`migration_event_id`),
  KEY `MigrationEventId` (`migration_event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8 COMMENT='migration cluster tbl';




-- Migration Shard Table
drop table if exists migration_shard_tbl;
CREATE TABLE `migration_shard_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `migration_cluster_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'migration cluster id',
  `shard_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'migration shard under specific migration cluster',
  `log` varchar(20480) NOT NULL DEFAULT '' COMMENT 'migration log',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `MigrationClusterId` (`migration_cluster_id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 COMMENT='migration events on specific shard';


-- Config Table
drop table if exists config_tbl;
CREATE TABLE `config_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `key` varchar(128) NOT NULL DEFAULT '' COMMENT 'key',
  `sub_key` varchar(128) NOT NULL DEFAULT '' COMMENT 'sub_key',
  `value` varchar(1024) DEFAULT '' COMMENT 'value',
  `until` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'for potential use',
  `latest_update_user` varchar(512) DEFAULT '' COMMENT 'latest person who update the config',
  `latest_update_ip` varchar(128) DEFAULT '' COMMENT 'ip where latest update occurs',
  `desc` varchar(1024) NOT NULL DEFAULT '' COMMENT 'desc',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  UNIQUE KEY `key_sub_key` (`key`,`sub_key`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 COMMENT='xpipe config';

INSERT INTO config_tbl (`key`, `value`, `desc`) VALUES ('sentinel.auto.process', 'true', '自动增删哨兵');
INSERT INTO config_tbl (`key`, `value`, `desc`) VALUES ('alert.system.on', 'true', '邮件报警系统开关');

-- Organization Table
drop table if exists organization_tbl;
CREATE TABLE `organization_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `org_id`  bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'organization id',
  `org_name` varchar(250) NOT NULL DEFAULT 'none' COMMENT 'organization name',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  UNIQUE KEY `org_id` (`org_id`),
  UNIQUE KEY `org_name` (`org_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Organization Info';
INSERT INTO organization_tbl (`org_id`, `org_name`) VALUES ('0', '');

-- Event Table
drop table if exists event_tbl;
CREATE TABLE `event_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `event_type`  varchar(20) NOT NULL DEFAULT 'none' COMMENT 'event type',
  `event_operator` varchar(128) NOT NULL DEFAULT 'none' COMMENT 'operator for the event',
  `event_operation` varchar(120) NOT NULL DEFAULT 'none' COMMENT 'event operation',
  `event_detail` varchar(512) NOT NULL DEFAULT 'none' COMMENT 'event details',
  `event_property` varchar(512) NOT NULL DEFAULT 'none' COMMENT 'potential property used for event',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `event_type` (`event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Event Info';

-- Route Table
drop table if exists route_tbl;
CREATE TABLE `route_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `route_org_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'organization id of route',
  `src_dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'source dc id',
  `dst_dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'destination dc id',
  `src_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'source proxies ids',
  `dst_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'destination proxies ids',
  `optional_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'optional relay proxies, ids separated by whitespace',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'active or not',
  `tag` varchar(128) NOT NULL DEFAULT '1' COMMENT 'tag for console or meta',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Route Info';

-- Proxy Table
drop table if exists proxy_tbl;
CREATE TABLE `proxy_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'dc id',
  `uri` varchar(256) NOT NULL DEFAULT 'TCP' COMMENT 'scheme, like PROXYTCP, PROXYTLS://127.0.0.1:8080, TCP://127.0.0.1:8090',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT 'active or not',
  `monitor_active` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'active or not',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Proxy Info';

-- az_tbl
drop table if exists az_tbl;
CREATE TABLE `az_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `dc_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'dc id',
  `active` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'active or not',
  `az_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'available zone name',
  `description` varchar(1024) NOT NULL DEFAULT '' COMMENT 'description for available zone',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_az_name` (`az_name`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='available zone Info';

-- redis_config_check_rule_tbl
drop table if exists redis_config_check_rule_tbl;
CREATE TABLE `redis_config_check_rule_tbl` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `check_type` varchar(128) NOT NULL DEFAULT '' COMMENT 'check type(info or config)',
  `param` varchar(256) NOT NULL DEFAULT '' COMMENT 'info of checkName, expectedVaule',
  `description` varchar(1024) NOT NULL DEFAULT '' COMMENT 'description for redis config check rule',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
  `deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='redis config check rule';

-- sentinel_group_tbl
drop table if exists sentinel_group_tbl;
CREATE TABLE `sentinel_group_tbl`
(
    `sentinel_group_id`   bigint(20) NOT NULL AUTO_INCREMENT COMMENT '哨兵组id',
    `cluster_type`        varchar(40)  NOT NULL DEFAULT '' COMMENT '该哨兵组适用的集群类型',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    `sentinel_description` varchar(100)  NOT NULL DEFAULT '',
    PRIMARY KEY (`sentinel_group_id`),
    KEY                   `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='哨兵组表';

-- sentinel_tbl
drop table if exists sentinel_tbl;
CREATE TABLE `sentinel_tbl`
(
    `sentinel_id`         bigint(20) NOT NULL AUTO_INCREMENT COMMENT '哨兵id',
    `dc_id`               bigint(20) NOT NULL DEFAULT '0' COMMENT '哨兵所在dc id',
    `sentinel_group_id`   bigint(20) NOT NULL DEFAULT '0' COMMENT '哨兵所属group id',
    `sentinel_ip`         varchar(40)  NOT NULL DEFAULT '0.0.0.0' COMMENT '哨兵ip',
    `sentinel_port`       int(11) NOT NULL DEFAULT '0' COMMENT '哨兵port',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`sentinel_id`),
    UNIQUE KEY `unniqueKey` (`sentinel_ip`,`sentinel_port`,`deleted`),
    KEY                   `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='哨兵表';
