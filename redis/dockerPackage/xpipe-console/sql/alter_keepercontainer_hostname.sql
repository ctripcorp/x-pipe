-- Online alter script for keepercontainer containerization (hostname identity)
-- Adds host_name columns to KEEPERCONTAINER_TBL and REDIS_TBL.
-- Physical machine rows keep the default empty string ('') — NOT NULL is required
-- because MySQL unique keys treat NULLs as never-equal, which would break the
-- (redis_ip, redis_port, host_name, deleted_at) dedup protection on REDIS_TBL.

ALTER TABLE `KEEPERCONTAINER_TBL`
  ADD COLUMN `keepercontainer_host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keepercontainer pod hostname, empty for physical machine' AFTER `keepercontainer_port`;

ALTER TABLE `REDIS_TBL`
  ADD COLUMN `host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keeper pod hostname, empty for physical machine' AFTER `redis_ip`,
  DROP INDEX `ip_port_deleted_at`,
  ADD UNIQUE KEY `ip_port_hostname_deleted_at` (`redis_ip`, `redis_port`, `host_name`, `deleted_at`);
