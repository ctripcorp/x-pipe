package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.SocketAware;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.core.protocal.Xsync.SIDNO_SEPARATOR;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public class FakeXsyncAction extends AbstractIoAction implements SocketAware {

    private BlockingQueue<String> writeCommands = new LinkedBlockingQueue<>();

    private FakeXsyncServer fakeXsyncServer;

    public FakeXsyncAction(FakeXsyncServer server, Socket socket) {
        super(socket);
        this.fakeXsyncServer = server;
    }

    @Override
    protected Object doRead(InputStream ins) throws IOException {
        return readLine(ins);
    }

    @Override
    protected void doWrite(OutputStream ous, Object readResult) throws IOException {
        String input = (String) readResult;
        if (StringUtil.isEmpty(input)) {
            return;
        }

        String[] args = input.trim().split(" ");
        if ("xsync".equalsIgnoreCase(args[0])) {
            List<String> interestedSidno = Arrays.asList(args[1].split(SIDNO_SEPARATOR));
            GtidSet excludedGtidSet = new GtidSet(args[2]);
            Object excludedVectorClock = null;
            if (args.length >= 4) excludedVectorClock = new Object(); // TODO: parse vector clock

            GtidSet rdbDataGtidSet = fakeXsyncServer.handleXsync(interestedSidno, excludedGtidSet, excludedVectorClock);
            try {
                if (null == rdbDataGtidSet) {
                    handlePartialSync(ous);
                } else {
                    handleFullSync(rdbDataGtidSet, ous);
                }
            } catch (InterruptedException e) {
                logger.warn("[doWrite][xsync] fail", e);
            }
        } else {
            ous.write("+OK\r\n".getBytes());
            ous.flush();
        }
    }

    private void handlePartialSync(OutputStream ous) throws IOException, InterruptedException {
        String resp = "+" + Xsync.PARTIAL_SYNC + "\r\n";
        ous.write(resp.getBytes());
        ous.flush();

        fakeXsyncServer.addCommandListener(this);
        writeCommands(ous);
    }

    private void handleFullSync(GtidSet rdbDataGtidSet, OutputStream ous) throws IOException, InterruptedException {
        String resp = String.format("+%s %s\r\n", Xsync.FULL_SYNC, rdbDataGtidSet.toString());
        ous.write(resp.getBytes());
        ous.flush();

        fakeXsyncServer.addCommandListener(this);
        ous.write(fakeXsyncServer.genRdbData());
        ous.flush();

        writeCommands(ous);
    }

    private void writeCommands(OutputStream ous) throws IOException, InterruptedException {

        startReadThread();

        while(true){

            String command = writeCommands.poll(10, TimeUnit.MILLISECONDS);
            if(getSocket().isClosed()){
                logger.info("[writeCommands][closed]");
                return;
            }
            if(command == null){
                continue;
            }
            String []args = command.trim().split(" ");
            if (0 == args.length) return;

            StringBuilder sb = new StringBuilder();
            sb.append("*").append(args.length).append("\r\n");
            for (String arg: args) {
                sb.append("$").append(arg.length()).append("\r\n");
                sb.append(arg).append("\r\n");
            }

            ous.write(sb.toString().getBytes());
            ous.flush();
        }
    }

    private void startReadThread() {

        new Thread(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                InputStream ins = getSocket().getInputStream();
                while(true){
                    int data = ins.read();
                    if(data == -1){
                        logger.info("[doRun]read -1, close socket:{}", getSocket());
                        getSocket().close();
                        return;
                    }
                }
            }
        }).start();
    }

    public void addCommands(String command){
        writeCommands.offer(command);
    }

    @Override
    public void setDead() {
        fakeXsyncServer.removeCommandListener(this);
    }
}
