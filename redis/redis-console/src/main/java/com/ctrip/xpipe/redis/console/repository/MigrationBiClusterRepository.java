package com.ctrip.xpipe.redis.console.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.MigrationBiClusterMapper;
import jakarta.annotation.Resource;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * @author lishanglin
 * date 2024/7/16
 */
@Repository
public class MigrationBiClusterRepository {

    @Resource
    private SqlSessionFactory sessionFactory;

    @Resource
    private MigrationBiClusterMapper migrationBiClusterMapper;

    public void insert(MigrationBiClusterEntity migrationBiClusterEntity) {
        migrationBiClusterMapper.insert(migrationBiClusterEntity);
    }

    public void batchInsert(List<MigrationBiClusterEntity> migrationBiClusterEntities) {
        SqlSession session = null;
        try {
            session = sessionFactory.openSession(ExecutorType.BATCH, false);
            MigrationBiClusterMapper batchMapper = session.getMapper(MigrationBiClusterMapper.class);
            migrationBiClusterEntities.forEach(batchMapper::insert);
            session.commit();
        } finally {
            if (null != session) session.close();
        }
    }

    public List<MigrationBiClusterEntity> selectAllByClusterIdAndOpTime(Collection<Long> clusterIds,
                                                                        Date from, Date to) {
        QueryWrapper<MigrationBiClusterEntity> query = new QueryWrapper<>();
        query.in(MigrationBiClusterEntity.CLUSTER_ID, clusterIds);
        query.ge(MigrationBiClusterEntity.OPERATION_TIME, from);
        query.lt(MigrationBiClusterEntity.OPERATION_TIME, to);

        return migrationBiClusterMapper.selectList(query);
    }

    public List<MigrationBiClusterEntity> selectAll() {
        return migrationBiClusterMapper.selectList(null);
    }

}
