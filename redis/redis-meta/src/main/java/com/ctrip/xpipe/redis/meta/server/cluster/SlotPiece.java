package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.List;

import com.ctrip.xpipe.api.command.Command;

/**
 * one piece in slot
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface SlotPiece extends Exportable, Importable{

	SlotPieceKey getKey();
	
	void addCommand(Command<?> command);
	
	List<Command<?>> pendingCommands();
	
}
