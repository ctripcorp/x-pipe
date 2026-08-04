-- T-11.6 / D27 / AC-18: LOGICAL_BU_* DataChange_LastTime timestamp → datetime
-- For existing environments that already created these tables with timestamp.
-- Fresh installs use xpipedemodbtables.sql (already datetime).
-- Safe to re-run: MODIFY to the same type is idempotent on MySQL 8.

ALTER TABLE `LOGICAL_BU_TBL`
  MODIFY COLUMN `DataChange_LastTime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time';

ALTER TABLE `LOGICAL_BU_ORG_TBL`
  MODIFY COLUMN `DataChange_LastTime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time';
