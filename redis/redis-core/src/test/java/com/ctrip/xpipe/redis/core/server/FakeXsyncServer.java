package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public class FakeXsyncServer extends AbstractLifecycle implements FakeXsyncHandler {

    private int port;

    private Server server;

    private FakeXsyncHandler innerXsyncHandler;

    private List<FakeXsyncAction> commandListeners = new LinkedList<>();

    public FakeXsyncServer(int port) {
        this(port, null);
    }

    public FakeXsyncServer(int port, FakeXsyncHandler xsyncHandler){
        this.port = port;
        this.innerXsyncHandler = xsyncHandler;
        this.server = new Server(port, new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {
                return new FakeXsyncAction(FakeXsyncServer.this, socket);
            }
        });
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        server.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        server.start();
    }

    @Override
    protected void doStop() throws Exception {
        server.stop();
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {
        server.dispose();
        super.doDispose();
    }

    @Override
    public GtidSet handleXsync(List<String> interestedSidno, GtidSet excludedGtidSet, Object excludedVectorClock) {
        if (null == innerXsyncHandler) return null;
        return innerXsyncHandler.handleXsync(interestedSidno, excludedGtidSet, excludedVectorClock);
    }

    @Override
    public byte[] genRdbData() {
        try (ByteArrayOutputStream rdb = new ByteArrayOutputStream()) {
            rdb.write("REDIS".getBytes(StandardCharsets.UTF_8));
            rdb.write("0009".getBytes(StandardCharsets.UTF_8));
            rdb.write(0xFA); // AUX 操作码
            writeLengthPrefixedString(rdb, "redis-ver");
            writeLengthPrefixedString(rdb, "5.0.0"); // 示例版本

            rdb.write(0xFE);
            rdb.write(0x00);
            rdb.write(0x00);
            writeLengthPrefixedString(rdb, "mykey");
            writeLengthPrefixedString(rdb, "hello");
            rdb.write(0xFF); // EOF 操作码
            byte[] checksum = new byte[] {
                    (byte) 0xA9, (byte) 0x10, (byte) 0x3A, (byte) 0x43,
                    (byte) 0x33, (byte) 0xE7, (byte) 0x7F, (byte) 0x92
            };
            rdb.write(checksum);
            return rdb.toByteArray();

        } catch (IOException e) {
            return new byte[0];
        }
    }

    private void writeLengthPrefixedString(ByteArrayOutputStream os, String str) throws IOException {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        os.write((byte) data.length);
        os.write(data);
    }

    public void setXsyncHandler(FakeXsyncHandler xsyncHandler) {
        this.innerXsyncHandler = xsyncHandler;
    }

    public int slaveCount() {
        return this.commandListeners.size();
    }

    public void propagate(String cmd) {
        addCommand(cmd);
    }

    private void addCommand(String command) {
        for(FakeXsyncAction listener :  commandListeners){
            listener.addCommands(command);
        }
    }

    public int getPort() {
        return port;
    }

    public void addCommandListener(FakeXsyncAction listener) {
        this.commandListeners.add(listener);
    }

    public void removeCommandListener(FakeXsyncAction listener) {
        this.commandListeners.remove(listener);
    }

}
