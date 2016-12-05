package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.meta.ActiveMetaStore;
import com.ctrip.xpipe.redis.keeper.store.meta.BackupMetaStore;
import com.ctrip.xpipe.redis.keeper.store.meta.InitMetaStore;

/**
 * @author marsqing
 *
 *         Jul 26, 2016 11:23:22 AM
 */
public class DefaultMetaStore implements InvocationHandler{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultMetaStore.class);
	
	private AtomicReference<MetaStore> metaStoreRef = new AtomicReference<MetaStore>(null);
	
	private File baseDir;
	
	private String keeperRunid;
	
	public DefaultMetaStore(File baseDir, String keeperRunid) {
		this.baseDir = baseDir;
		this.keeperRunid = keeperRunid;
		loadMetaStore();
	}
	
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		
		String methodName = method.getName(); 
		if(methodName.equals(MetaStore.METHOD_BECOME_ACTIVE)){
			becomeActive();
			return null;
		}else if(methodName.equals(MetaStore.METHOD_BECOME_BACKUP)){
			becomeBackup();
			return null;
		}else{
			return method.invoke(metaStoreRef.get(), args);
		}
	}

	public static MetaStore createMetaStore(File baseDir, String keeperRunid){
		
		return (MetaStore) Proxy.newProxyInstance(DefaultMetaStore.class.getClassLoader(), 
				new Class[]{MetaStore.class}, new DefaultMetaStore(baseDir, keeperRunid));
	}

	private void loadMetaStore() {
		
		ReplicationStoreMeta meta = null;
		try {
			meta = AbstractMetaStore.loadMetaCreateIfEmpty(baseDir, MetaStore.META_FILE);
			if(meta.getKeeperState() == null){
				initStore();
			}else if(meta.getKeeperState() == KeeperState.ACTIVE){
				activeStore();
			}else if(meta.getKeeperState() == KeeperState.BACKUP){
				backupStore();
			}else{
				throw new IllegalStateException("unsupported state:" + meta.getKeeperState());
			}
		} catch (IOException e) {
			throw new IllegalStateException("loadMetaStore " + baseDir + " " + keeperRunid, e);
		}
	}

	private MetaStore initStore() {
		MetaStore metaStore = new InitMetaStore(baseDir, keeperRunid); 
		metaStoreRef.set(metaStore);
		return metaStore;
	}

	private MetaStore backupStore() {
		
		MetaStore metaStore = new BackupMetaStore(baseDir, keeperRunid); 
		metaStoreRef.set(metaStore);
		return metaStore;
	}

	private MetaStore activeStore() {
		
		MetaStore metaStore = new ActiveMetaStore(baseDir, keeperRunid); 
		metaStoreRef.set(metaStore);
		return metaStore;
	}

	public synchronized void becomeBackup() throws IOException {

		if(metaStoreRef.get() instanceof BackupMetaStore){
			logger.info("[becomeBackup][already backup]{}", baseDir);
			return ;
		}
		
		logger.info("[becomeBackup]{}", baseDir);
		MetaStore metaStore = backupStore();
		metaStore.setKeeperState(keeperRunid, KeeperState.BACKUP);
	}

	public synchronized void becomeActive() throws IOException {
		
		if(metaStoreRef.get() instanceof ActiveMetaStore){
			logger.info("[becomeActive][already active]{}", baseDir);
			return ;
		}
		
		logger.info("[becomeActive]{}", baseDir);
		MetaStore metaStore = activeStore();
		metaStore.setKeeperState(keeperRunid, KeeperState.ACTIVE);
	}
}
