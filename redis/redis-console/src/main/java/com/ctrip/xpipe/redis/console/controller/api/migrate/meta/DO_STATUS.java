package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public enum  DO_STATUS {

    INITED,
    MIGRATING,
    FAIL,
    SUCCESS,
    ROLLBACK,
    ROLLBACKSUCCESS;

    public static DO_STATUS fromMigrationStatus(MigrationStatus migrationStatus){

        DO_STATUS do_status = null;

        switch (migrationStatus){
            case Initiated:
                do_status = INITED;
                break;
            case Checking:
                do_status = MIGRATING;
                break;
            case CheckingFail:
                do_status = FAIL;
                break;
            case Migrating:
                do_status = MIGRATING;
                break;
            case PartialSuccess:
                do_status = FAIL;
                break;
            case RollBack:
                do_status = ROLLBACK;
                break;
            case Publish:
                do_status = MIGRATING;
                break;
            case PublishFail:
                do_status = FAIL;
                break;
            case Success:
                do_status = SUCCESS;
                break;
            case Aborted:
                do_status = ROLLBACKSUCCESS;
                break;
            case ForceEnd:
                do_status = SUCCESS;
                break;
            default:
                throw new IllegalArgumentException(String.format("%s", migrationStatus));
        }
        return do_status;
    }

}
