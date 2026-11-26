-- Logical BU keeper priority for TFS <-> BM switching.
-- NULL (default): every keeper uses REDIS_TBL.keeper_priority (current behavior).
-- Non-NULL: TFS keepers on this BU's keepercontainers use this value; BM keepers keep REDIS_TBL.keeper_priority.
-- Fresh installs use xpipedemodbtables.sql (already contains the column).

ALTER TABLE `LOGICAL_BU_TBL`
  ADD COLUMN `priority` int(11) DEFAULT NULL COMMENT 'TFS keeper priority, NULL means use keeper_priority' AFTER `tfs_fs_id`;
