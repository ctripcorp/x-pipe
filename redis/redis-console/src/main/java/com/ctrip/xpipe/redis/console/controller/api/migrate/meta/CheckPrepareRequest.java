package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
public class CheckPrepareRequest extends AbstractRequestMeta{
	
	private boolean isForce;
	private String fromIdc;
	private List<String>  clusters;
	
	public List<String> getClusters() {
		return clusters;
	}
	public void setClusters(List<String> clusters) {
		this.clusters = clusters;
	}
	public boolean isForce() {
		return isForce;
	}
	public void setForce(boolean isForce) {
		this.isForce = isForce;
	}
	public String getFromIdc() {
		return fromIdc;
	}
	public void setFromIdc(String fromIdc) {
		this.fromIdc = fromIdc;
	}
	
}
