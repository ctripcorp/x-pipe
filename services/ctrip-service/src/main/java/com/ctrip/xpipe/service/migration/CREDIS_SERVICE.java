package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.utils.UrlUtils;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public enum CREDIS_SERVICE {

	MIGRATION_PUBLISH(PATH.PATH_MIGRATION_PUBLISH),
	SWITCH_STATUS(PATH.PATH_SWITCH_STATUS),
	QUERY_STATUS(PATH.PATH_QUERY_STATUS),
	QUERY_CLUSTER(PATH.PATH_QUERY_CLUSTER);

	private String path;

	CREDIS_SERVICE(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public String getRealPath(String host) {

		if (!host.startsWith("http")) {
			host += "http://";
		}
		return UrlUtils.format(String.format("%s/%s/%s", host, PATH.PATH_PREFIX, getPath()));
	}

	public static class PATH {

		public static final String PATH_PREFIX = "/";
		public static final String PATH_MIGRATION_PUBLISH = "/KeeperApi/primarydc/{clusterName}/{primaryDcName}";
		public static final String PATH_SWITCH_STATUS  = "/KeeperApi/SwitchReadStatus";
		public static final String PATH_QUERY_STATUS = "KeeperApi/QueryReadStatus";
		public static final String PATH_QUERY_CLUSTER = "KeeperApi/querycluster/{clusterName}/";
	}
}
