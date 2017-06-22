package com.ctrip.xpipe.redis.console.controller.migrate.meta;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
public class CheckPrepareResponseMeta extends AbstractMeta{

	private int ticketId;
	
	private List<CheckPrepareClusterResponse>  results = new LinkedList<>();
	
	public CheckPrepareResponseMeta(){
		
	}

	public CheckPrepareResponseMeta(int ticketId, List<CheckPrepareClusterResponse>  results){
		this.ticketId = ticketId;
		this.results = results;
	}

	public int getTicketId() {
		return ticketId;
	}

	public void setTicketId(int ticketId) {
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
