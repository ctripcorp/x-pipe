package com.ctrip.xpipe.observer;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class NodeDeleted<V> extends AbstractEvent<V>{
	
	private V node;
	public NodeDeleted(V node){
		this.node = node;
	}
	
	public V getNode() {
		return node;
	}

	
	@Override
	public String toString() {
		return "Deleted:" + node;
	}
}
