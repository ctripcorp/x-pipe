package com.ctrip.xpipe.observer;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class NodeModified<V> extends AbstractEvent<V>{
	
	private V oldNode, newNode;
	public NodeModified(V oldNode, V newNode){
		this.oldNode = oldNode;
		this.newNode = newNode;
	}

	public V getOldNode() {
		return oldNode;
	}
	
	public V getNewNode() {
		return newNode;
	}

	@Override
	public String toString() {
		return "Modified:" + oldNode + "->" + newNode;
	}
}
