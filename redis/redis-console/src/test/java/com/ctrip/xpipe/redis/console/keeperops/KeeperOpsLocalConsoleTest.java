package com.ctrip.xpipe.redis.console.keeperops;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.redis.console.AppTest;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * Local console (H2 in memory, port 8080) for manually testing logical BU keeper-ops.
 * Not part of AllTests; run explicitly:
 * <pre>
 * mvn -o test -pl redis/redis-console -Dtest=KeeperOpsLocalConsoleTest#startConsole8080 -DfailIfNoTests=false -Djacoco.skip=true
 * </pre>
 * Stop: create the file {@code ${java.io.tmpdir}/keeper-ops-console.stop}.
 *
 * <p>Test data (on top of apptest.sql), all in dc {@code jq}, cluster {@code cluster1}:
 * <ul>
 *     <li>BU 42: shard1 → TFS KC 106 (127.0.0.1:6000) + BM KC 100 (6001), BM KC 101 (6002)</li>
 *     <li>BU 43: shard2 → TFS KC 120 (7000) + BM KC 102 (7001)</li>
 * Both are in IDC jq, so one switchBM for [42, 43] calls {@code /api/dcmeta/reload} on jq once.
 * </ul>
 * MetaServer of {@code jq} is http://127.0.0.1:9747, see {@link KeeperOpsFakeMetaServer}.
 */
public class KeeperOpsLocalConsoleTest extends AppTest {

    public static final File STOP_FILE = new File(System.getProperty("java.io.tmpdir"), "keeper-ops-console.stop");

    static final String META_SERVERS = "{ \"jq\" : \"http://127.0.0.1:9747\", \"oy\" : \"http://127.0.0.1:9748\" }";

    /**
     * AppTest sets the "metaservers" system property, but dc.properties (DataCenterConfigBean) wins over it,
     * so the fake MetaServer address has to be written into that config directly.
     */
    @Before
    public void injectLocalMetaServers() throws Exception {
        Object config = ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.DATA_CENTER_CONFIG_NAME);
        Field field = config.getClass().getDeclaredField("properties");
        field.setAccessible(true);
        ((Properties) field.get(config)).setProperty("metaservers", META_SERVERS);
    }

    /** Surefire closes stdin, so AppTest's "press any key" returns at once; block until the stop file exists. */
    @Override
    @After
    public void afterAppTest() throws IOException {
        STOP_FILE.delete();
        logger.info("[KeeperOpsLocalConsole] running, create {} to stop", STOP_FILE.getAbsolutePath());
        while (!STOP_FILE.exists()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        STOP_FILE.delete();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return super.prepareDatas() + "\n"
                // BU 42 / jq/cluster1/shard1: 1 TFS + 2 BM
                + "update REDIS_TBL set keepercontainer_id=106 where id=1;\n"
                + "update REDIS_TBL set keepercontainer_id=100 where id=2;\n"
                + "insert into REDIS_TBL (id,run_id,dc_cluster_shard_id,redis_ip,redis_port,redis_role,master,redis_master,keepercontainer_id) "
                + "values (9001,'ffffffffffffffffffffffffffffffffffffffff',1,'127.0.0.1',6002,'keeper',0,0,101);\n"
                // BU 43 / jq/cluster1/shard2: 1 TFS + 1 BM
                + "insert into LOGICAL_BU_TBL (id, name, tfs_fs_id, active, description) values (43,'test-bu-43','tfs-fs-43',1,'keeper-ops local: blocked');\n"
                + "insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,keepercontainer_org_id,keepercontainer_disk_type,logical_bu_id) "
                + "values (120,1,'127.0.10.20',8080,1,1,'TFS_1',43);\n"
                + "update REDIS_TBL set keepercontainer_id=120 where id=10;\n"
                + "update REDIS_TBL set keepercontainer_id=102 where id=11;\n";
    }
}
