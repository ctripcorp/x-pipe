package com.ctrip.xpipe.redis.console.repository;

import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import com.ctrip.xpipe.redis.console.mapper.MigrationBiClusterMapper;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
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

    public List<MigrationBiClusterEntity> selectAll() {
        return migrationBiClusterMapper.selectList(null);
    }

}
