package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * @author hailu
 * @date 2024/5/14 13:59
 */
public class FakePsyncServer extends AbstractLifecycle implements FakePsyncHandler{

    private int port;

    private Server server;

    private FakePsyncHandler innerPsyncHandler;

    private List<FakePsyncAction> commandListeners = new LinkedList<>();

    public FakePsyncServer(int port) {
        this(port, null);
    }

    public FakePsyncServer(int port, FakePsyncHandler psyncHandler){
        this.port = port;
        this.innerPsyncHandler = psyncHandler;
        this.server = new Server(port, new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {
                return new FakePsyncAction(FakePsyncServer.this, socket);
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
    public byte[] genRdbData() {
        return new byte[0];
    }

    @Override
    public Long handlePsync(String replId, long offset) {
        if (null == innerPsyncHandler) return null;
        return innerPsyncHandler.handlePsync(replId, offset);
    }

    public void setPsyncHandler(FakePsyncHandler psyncHandler) {
        this.innerPsyncHandler = psyncHandler;
    }

    public int slaveCount() {
        return this.commandListeners.size();
    }

    public void propagate(String cmd) {
        addCommand(cmd);
    }

    private void addCommand(String command) {
        for(FakePsyncAction listener :  commandListeners){
            listener.addCommands(command);
        }
    }

    public int getPort() {
        return port;
    }

    public void addCommandListener(FakePsyncAction listener) {
        this.commandListeners.add(listener);
    }

    public void removeCommandListener(FakePsyncAction listener) {
        this.commandListeners.remove(listener);
    }

}
