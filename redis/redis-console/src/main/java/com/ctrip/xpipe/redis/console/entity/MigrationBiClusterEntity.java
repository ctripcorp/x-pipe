package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ctrip.xpipe.redis.console.entity.BaseEntity;
import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * bi cluster migration record
 * </p>
 *
 * @author mybatis-generator
 * @since 2024-07-16
 */
@TableName("migration_bi_cluster_tbl")
public class MigrationBiClusterEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

      /**
     * primary key
     */
        @TableId(value = "id", type = IdType.AUTO)
      private Long id;

      /**
     * reference bi cluster
     */
      @TableField("cluster_id")
    private Long clusterId;

      /**
     * migration time
     */
      @TableField("operation_time")
    private Date operationTime;

      /**
     * migration operator
     */
      @TableField("operator")
    private String operator;

      /**
     * migration status
     */
      @TableField("status")
    private String status;

      /**
     * migration publish information
     */
      @TableField("publish_info")
    private String publishInfo;
    
    public Long getId() {
        return id;
    }

      public MigrationBiClusterEntity setId(Long id) {
          this.id = id;
          return this;
      }
    
    public Long getClusterId() {
        return clusterId;
    }

      public MigrationBiClusterEntity setClusterId(Long clusterId) {
          this.clusterId = clusterId;
          return this;
      }
    
    public Date getOperationTime() {
        return operationTime;
    }

      public MigrationBiClusterEntity setOperationTime(Date operationTime) {
          this.operationTime = operationTime;
          return this;
      }
    
    public String getOperator() {
        return operator;
    }

      public MigrationBiClusterEntity setOperator(String operator) {
          this.operator = operator;
          return this;
      }
    
    public String getStatus() {
        return status;
    }

      public MigrationBiClusterEntity setStatus(String status) {
          this.status = status;
          return this;
      }
    
    public String getPublishInfo() {
        return publishInfo;
    }

      public MigrationBiClusterEntity setPublishInfo(String publishInfo) {
          this.publishInfo = publishInfo;
          return this;
      }
  
    public static final String ID = "id";
  
    public static final String CLUSTER_ID = "cluster_id";
  
    public static final String OPERATION_TIME = "operation_time";
  
    public static final String OPERATOR = "operator";
  
    public static final String STATUS = "status";
  
    public static final String PUBLISH_INFO = "publish_info";
  
    @Override
    public String toString() {
        return "MigrationBiClusterEntity{" +
              "id = " + id +
                  ", clusterId = " + clusterId +
                  ", operationTime = " + operationTime +
                  ", operator = " + operator +
                  ", status = " + status +
                  ", publishInfo = " + publishInfo +
              "}";
    }
}
