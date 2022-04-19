package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.Gate;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReader implements CommandReader {

    private File curFile;

    private long curPosition;

    private ReferenceFileChannel referenceFileChannel;

    private CommandStore commandStore;

    private OffsetNotifier offsetNotifier;

    private AtomicLong flying = new AtomicLong(0);

    private long flyingThreshold;

    private Gate gate;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandReader.class);

    public OffsetCommandReader(File curFile, long globalPosition, long filePosition, CommandStore commandStore, OffsetNotifier offsetNotifier, long flyingThreshold)
            throws IOException {
        this.flyingThreshold = flyingThreshold;
        this.commandStore = commandStore;
        this.offsetNotifier = offsetNotifier;
        gate = new Gate(commandStore.simpleDesc());
        this.curFile = curFile;
        curPosition = globalPosition;
        referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curFile), filePosition);
    }

    @Override
    public void close() throws IOException {
        commandStore.removeReader(this);
        referenceFileChannel.close();
    }

    @Override
    public ReferenceFileRegion read() throws IOException {
        try {
            gate.tryPass();
            offsetNotifier.await(curPosition);
            readNextFileIfNecessary();
        } catch (InterruptedException e) {
            logger.info("[read]", e);
            Thread.currentThread().interrupt();
        }

        ReferenceFileRegion referenceFileRegion = referenceFileChannel.readTilEnd();

        curPosition += referenceFileRegion.count();

        referenceFileRegion.setTotalPos(curPosition);

        if (referenceFileRegion.count() < 0) {
            logger.error("[read]{}", referenceFileRegion);
        }

        checkCloseGate(flying.incrementAndGet());
        return referenceFileRegion;
    }

    private void checkCloseGate(long current) {

        debugPrint(current);

        if(gate.isOpen() && (current >= flyingThreshold)){
            logger.info("[increaseFlying][close gate]{}, {}", gate, current);
            gate.close();
            //just in case, before gate.close(), all flushed
            checkOpenGate(flying.get());
        }
    }

    private void debugPrint(long current) {

        if(logger.isDebugEnabled() && (current > 4)){
            int intCurrent = (int) current;
            if((intCurrent & (intCurrent-1)) == 0){
                logger.debug("[flying]{}, {}", gate, current);
            }
        }
    }

    private void checkOpenGate(long current){

        debugPrint(current);

        if(!gate.isOpen() && (current <= (flyingThreshold >> 2))){
            logger.info("[decreaseFlying][open gate]{}, {}", gate, flying);
            gate.open();
        }
    }

    @Override
    public void flushed(ReferenceFileRegion referenceFileRegion){
        checkOpenGate(flying.decrementAndGet());
    }

    private void readNextFileIfNecessary() throws IOException {
        commandStore.makeSureOpen();

        if (!referenceFileChannel.hasAnythingToRead()) {
            // TODO notify when next file ready
            CommandFile nextCommandFile = commandStore.findNextFile(curFile);
            if (nextCommandFile != null) {
                curFile = nextCommandFile.getFile();
                referenceFileChannel.close();
                referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curFile));
            }
        }
    }



    public File getCurFile() {
        return curFile;
    }

    @Override
    public String toString() {
        return "curFile:" + curFile;
    }

}
