package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableLogic;

import java.io.Serializable;
import java.util.Date;

public class BaseEntity implements Serializable {

    private static final long serialVersionUID = -2872237585261489993L;

    @TableField(value = "DataChange_LastTime", fill = FieldFill.INSERT_UPDATE)
    private Date dataChangeLastTime;

    @TableLogic
    @TableField("deleted")
    private boolean deleted;

    public Date getDataChangeLastTime() {
        return dataChangeLastTime;
    }

    public void setDataChangeLastTime(Date dataChangeLastTime) {
        this.dataChangeLastTime = dataChangeLastTime;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public static final String DATA_CHANGE_LAST_TIME = "DataChange_LastTime";

    public static final String DELETED = "deleted";

    @Override
    public String toString() {
        return "BaseEntity{" + "dataChangeLastTime=" + dataChangeLastTime + ", deleted=" + deleted + '}';
    }
}
