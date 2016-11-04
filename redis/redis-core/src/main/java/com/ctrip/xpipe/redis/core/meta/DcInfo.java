package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 *         Nov 3, 2016
 */
public class DcInfo {

	private String metaServerAddress;
	
	public DcInfo(){
		
	}

	public DcInfo(String metaServerAddress){
		this.metaServerAddress = metaServerAddress;		
	}

	public String getMetaServerAddress() {
		return metaServerAddress;
	}

	public void setMetaServerAddress(String metaServerAddress) {
		this.metaServerAddress = metaServerAddress;
	}
	
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof DcInfo) {
			DcInfo other = (DcInfo) obj;
			if (!ObjectUtils.equals(metaServerAddress, other.metaServerAddress)) {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		
		return ObjectUtils.hashCode(metaServerAddress);
	}
	
	@Override
	public String toString() {
		
		return String.format("metaServerAddress:%s", this.metaServerAddress);
	}
}
