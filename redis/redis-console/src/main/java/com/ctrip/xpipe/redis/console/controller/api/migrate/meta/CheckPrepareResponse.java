package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
public class CheckPrepareResponse extends AbstractMeta{

	private long ticketId;
	
	private List<CheckPrepareClusterResponse>  results = new LinkedList<>();
	
	public CheckPrepareResponse(){
		
	}

	public CheckPrepareResponse(int ticketId, List<CheckPrepareClusterResponse>  results){
		this.ticketId = ticketId;
		this.results = results;
	}

	public long getTicketId() {
		return ticketId;
	}

	public void setTicketId(long ticketId) {
		this.ticketId = ticketId;
	}

	public List<CheckPrepareClusterResponse> getResults() {
		return results;
	}

	public synchronized void addCheckPrepareClusterResponse(CheckPrepareClusterResponse response){

		for(CheckPrepareClusterResponse result : results){
			if(result.getClusterName().equals(response.getClusterName())){
				throw new IllegalArgumentException(
						String.format("already has response:%s, but given:%s", result, response)
				);
			}
		}
		this.results.add(response);
	}

	public void addCheckPrepareClusterResponse(List<CheckPrepareClusterResponse> responses){
		responses.forEach(( response ) -> addCheckPrepareClusterResponse(response));
	}
}
