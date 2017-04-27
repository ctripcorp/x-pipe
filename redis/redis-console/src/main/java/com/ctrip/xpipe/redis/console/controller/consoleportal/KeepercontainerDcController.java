package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;


/**
 * @author shyin
 *
 * Aug 22, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeepercontainerDcController extends AbstractConsoleController{

	@Autowired
	private DcService dcService;

	@Autowired
	private KeepercontainerService keepercontainerService;

	@Autowired
	private RedisService redisService;

	@RequestMapping(value = "/dcs/{dcName}/activekeepercontainers", method = RequestMethod.GET)
	public List<KeepercontainerTbl> findKeeperContainer(@PathVariable String dcName){
		return keepercontainerService.findAllActiveByDcName(dcName);
	}


	@RequestMapping(value = "/dcs/{dcName}/availablekeepers", method = RequestMethod.POST)
	public List<RedisTbl> findAvailableKeepers(@PathVariable String dcName,
											   @RequestBody(required = false) ShardModel shardModel){
		final int returnCount = 2;

		logger.debug("[findAvailableKeepers]{}, {}", dcName, shardModel);

		List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCount(dcName);
		if(keeperCount.size() < returnCount){
			throw new IllegalStateException("all keepers size:" + keeperCount + ", but we need:" + returnCount);
		}

		List<RedisTbl> result = new LinkedList<>();

		//find available port
		keeperCount.forEach(new Consumer<KeepercontainerTbl>() {
			@Override
			public void accept(KeepercontainerTbl keepercontainerTbl) {

				RedisTbl redisTbl = new RedisTbl();
				redisTbl.setKeepercontainerId(keepercontainerTbl.getKeepercontainerId());
				redisTbl.setRedisIp(keepercontainerTbl.getKeepercontainerIp());

				int port = findAvailablePort(keepercontainerTbl, shardModel, result);
				redisTbl.setRedisPort(port);
				result.add(redisTbl);
			}
		});
		return result;
	}

	private int findAvailablePort(KeepercontainerTbl keepercontainerTbl, ShardModel shardModel, List<RedisTbl> result) {

		int port = RedisProtocol.REDIS_PORT_DEFAULT;
		if(shardModel != null && shardModel.getRedises().size() > 0){
			port = shardModel.getRedises().get(0).getRedisPort();
		}

		String ip = keepercontainerTbl.getKeepercontainerIp();
		for(;;port++){

			if(alreadySelected(ip, port, result)){
				continue;
			}
			if(shardModel != null && existOnConsole(ip, port, shardModel.getKeepers())){
				continue;
			}
			if(existInDb(ip, port)){
				continue;
			}
			break;
		}
		return port;
	}

	private boolean alreadySelected(String ip, int port, List<RedisTbl> result) {

		for(RedisTbl redisTbl : result){
			if(redisTbl.getRedisIp().equalsIgnoreCase(ip) && redisTbl.getRedisPort() == port){
				return true;
			}
		}
		return false;
	}

	private boolean existInDb(String keepercontainerIp, int port) {

		return redisService.findWithIpPort(keepercontainerIp, port) != null;
	}

	private boolean existOnConsole(String keepercontainerIp, int port, List<RedisTbl> keepers) {

		for(RedisTbl redisTbl : keepers){
			if(redisTbl.getRedisIp().equalsIgnoreCase(keepercontainerIp)
					&& redisTbl.getRedisPort() == port){
				return true;
			}
		}
		return false;
	}
}
