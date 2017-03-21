package com.ctrip.xpipe.redis.console.controller.pub;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
public class CheckRetMeta extends AbstractMeta{

	private int ticketId;
	
	private List<CheckClusterRet>  results;
	
	public CheckRetMeta(){
		
	}

	public CheckRetMeta(int ticketId, List<CheckClusterRet>  results){
		this.ticketId = ticketId;
		this.results = results;
	}

	public int getTicketId() {
		return ticketId;
	}

	public void setTicketId(int ticketId) {
		this.ticketId = ticketId;
	}

	public List<CheckClusterRet> getResults() {
		return results;
	}

	public void setResults(List<CheckClusterRet> results) {
		this.results = results;
	}

	public static class CheckClusterRet extends AbstractMeta{
		
		private String clusterName;
		private boolean success;
		private String msg;
		
		public CheckClusterRet(){
			
		}

		public CheckClusterRet(String clusterName, boolean success){
			this.clusterName = clusterName;
			this.success = success;
		}

		public String getClusterName() {
			return clusterName;
		}
		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}
		public boolean isSuccess() {
			return success;
		}
		public void setSuccess(boolean success) {
			this.success = success;
		}
		public String getMsg() {
			return msg;
		}
		public void setMsg(String msg) {
			this.msg = msg;
		}
	}
}
