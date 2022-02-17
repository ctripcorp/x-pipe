package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.utils.UrlUtils;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public enum CREDIS_SERVICE {

	MIGRATION_PRE_CHECK(PATH.PATH_MIGRATION_PRE_CHECK),
	MIGRATION_PUBLISH(PATH.PATH_MIGRATION_PUBLISH),
	SWITCH_STATUS(PATH.PATH_SWITCH_STATUS),
	QUERY_STATUS(PATH.PATH_QUERY_STATUS),
	QUERY_CLUSTER(PATH.PATH_QUERY_CLUSTER),
	QUERY_DC_META(PATH.PATH_QUERY_DC_META);

	private String path;

	CREDIS_SERVICE(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public String getRealPath(String host) {

		if (!host.startsWith("http")) {
			host = "http://" + host;
		}
		return UrlUtils.format(String.format("%s/%s/%s", host, PATH.PATH_PREFIX, getPath()));
	}

	public static class PATH {

		public static final String PATH_PREFIX = "/";
		public static final String PATH_MIGRATION_PRE_CHECK = "/keeperApi/checkcluster/{clusterName}";
		public static final String PATH_MIGRATION_PUBLISH = "/keeperApi/primarydc/{clusterName}/{primaryDcName}";
		public static final String PATH_SWITCH_STATUS  = "/keeperApi/switchReadStatus";
		public static final String PATH_QUERY_STATUS = "keeperApi/queryReadStatus";
		public static final String PATH_QUERY_CLUSTER = "keeperApi/querycluster";
		public static final String PATH_QUERY_DC_META = "config/getNonOperatingIdcClusters";
	}
}
