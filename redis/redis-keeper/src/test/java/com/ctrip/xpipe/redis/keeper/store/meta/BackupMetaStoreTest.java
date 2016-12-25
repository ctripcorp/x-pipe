package com.ctrip.xpipe.redis.keeper.store.meta;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;


/**
 * @author wenchao.meng
 *
 * Dec 5, 2016
 */
public class BackupMetaStoreTest extends AbstractRedisKeeperTest{
	
	private BackupMetaStore backupMetaStore;
	private String masterRunid = randomKeeperRunid();
	private long beginOffset = randomInt();
	private String rdbFile = randomString(10);
	private long rdbFileSize = randomInt();
	private String cmdFilePrefix = randomString(10);
	
	@Before
	public void beforeBackupMetaStoreTest() throws IOException{
		String baseDir = getTestFileDir();
		backupMetaStore = new BackupMetaStore(new File(baseDir), randomKeeperRunid());
		backupMetaStore.rdbBegun(masterRunid, beginOffset, rdbFile, new LenEofType(rdbFileSize), cmdFilePrefix);
	}
	
	@Test
	public void testRdbBegun() throws IOException{
		
		ReplicationStoreMeta  meta = backupMetaStore.dupReplicationStoreMeta();
		
		Assert.assertEquals(masterRunid, meta.getKeeperRunid());
		Assert.assertEquals(beginOffset, meta.getKeeperBeginOffset());
		Assert.assertEquals(rdbFile, meta.getRdbFile());
		Assert.assertEquals(rdbFileSize, meta.getRdbFileSize());
		Assert.assertEquals(cmdFilePrefix, meta.getCmdFilePrefix());
	}
	
	@Test
	public void testSaveKinfo() throws IOException{
		
		ReplicationStoreMeta kinfo = new ReplicationStoreMeta();
		
		kinfo.setMasterAddress(new DefaultEndPoint("localhost", randomPort()));
		kinfo.setMasterRunid(randomKeeperRunid());
		kinfo.setBeginOffset((long) randomInt());
		kinfo.setKeeperBeginOffset(randomInt());
		
		backupMetaStore.saveKinfo(kinfo);
		
		ReplicationStoreMeta  meta = backupMetaStore.dupReplicationStoreMeta();
		Assert.assertEquals(kinfo.getMasterAddress(), meta.getMasterAddress());
		Assert.assertEquals(kinfo.getMasterRunid(), meta.getMasterRunid());
		Assert.assertEquals((Long)(kinfo.getBeginOffset() + (beginOffset - kinfo.getKeeperBeginOffset())), meta.getBeginOffset());
	}
	
	@Test
	public void testRecoverFromPrevious() throws IOException{
		
		String currentFile = "meta.json";
		String masterFile = "root-BACKUP_REDIS_MASTER.json";
		File oldMetaBaseDir = new File(getTestFileDir());
		
		prepareTestData(new File(oldMetaBaseDir, currentFile), new File(oldMetaBaseDir, masterFile));
		
		ReplicationStoreMeta metaCurrent = AbstractMetaStore.loadMetaCreateIfEmpty(oldMetaBaseDir, currentFile);
		ReplicationStoreMeta metaMaster = AbstractMetaStore.loadMetaCreateIfEmpty(oldMetaBaseDir, masterFile);
		
		BackupMetaStore backupMetaStore = new BackupMetaStore(oldMetaBaseDir, metaCurrent.getKeeperRunid());
		
		ReplicationStoreMeta meta = backupMetaStore.dupReplicationStoreMeta();
		
		Assert.assertEquals(metaMaster.getMasterRunid(), meta.getMasterRunid());
		Assert.assertEquals(metaMaster.getMasterAddress(), meta.getMasterAddress());
		Assert.assertEquals((Long)metaMaster.getKeeperBeginOffset(), metaCurrent.getBeginOffset());
		
		Assert.assertEquals((Long)metaMaster.getKeeperBeginOffset() + (metaCurrent.getBeginOffset() - metaMaster.getKeeperBeginOffset()), metaCurrent.getKeeperBeginOffset());
		Assert.assertEquals((Long)(metaCurrent.getBeginOffset() + (metaCurrent.getRdbLastKeeperOffset() - metaCurrent.getKeeperBeginOffset())), 
				metaCurrent.getRdbLastKeeperOffset());

		//again
		backupMetaStore = new BackupMetaStore(oldMetaBaseDir, metaCurrent.getKeeperRunid());
		Assert.assertEquals(meta, backupMetaStore.dupReplicationStoreMeta());

	}

	private void prepareTestData(File meta, File metaMaster) throws IOException {
		
		FileUtils.write(meta, "{\"beginOffset\":11065,\"cmdFilePrefix\":\"cmd_4d6e1c36-ffc6-4180-b7fb-68f37e6da9dd_\","
				+ "\"keeperBeginOffset\":11065,"
				+ "\"keeperRunid\":\"65dafa9511809d0968b9391dea93d2c3de178fe2\","
				+ "\"keeperState\":\"BACKUP\","
				+ "\"masterAddress\":{\"rawUrl\":\"redis://10.15.138.237:6380\",\"socketAddress\":{\"address\":\"10.15.138.237\",\"port\":6380}}"
				+ ",\"masterRunid\":\"65dafa9511809d0968b9391dea93d2c3de178fe2\","
				+ "\"rdbFile\":\"rdb_1480569903255_dd96f335-a22f-49d5-9827-fb60933e17eb\","
				+ "\"rdbFileSize\":10070131544,\"rdbLastKeeperOffset\":11064}");

		FileUtils.write(metaMaster, "{\"beginOffset\":97781,\"cmdFilePrefix\":\"cmd_ee89913f-96f7-47fe-b513-407f1730a3c7_\","
				+ "\"keeperBeginOffset\":11065,"
				+ "\"keeperRunid\":\"65dafa9511809d0968b9391dea93d2c3de178fe2\""
				+ ",\"keeperState\":\"ACTIVE\","
				+ "\"masterAddress\":{\"rawUrl\":\"redis://10.15.138.81:6380\",\"socketAddress\":{\"address\":\"10.15.138.237\",\"port\":6380}}"
				+ ",\"masterRunid\":\"c801e4ad5452db0763460e669ffa30d4ec36c52b\","
				+ "\"rdbFile\":\"rdb_1480569902241_2c448233-e1d9-4926-97b9-caa355f508ac\","
				+ "\"rdbFileSize\":10070131544,\"rdbLastKeeperOffset\":11064}");
	}
}
