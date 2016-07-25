package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface Slot extends Exportable, Importable{
	
	int getId();
	
	List<SlotPiece>  getSlotPieces();
	
	boolean hasPiece(SlotPieceKey key);
	

}
