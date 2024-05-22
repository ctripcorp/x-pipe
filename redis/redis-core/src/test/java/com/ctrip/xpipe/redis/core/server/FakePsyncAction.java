package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGenerator;
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

import static com.ctrip.xpipe.redis.core.protocal.Sync.SIDNO_SEPARATOR;

/**
 * @author hailu
 * @date 2024/5/14 14:05
 */
public class FakePsyncAction extends AbstractIoAction implements SocketAware {

    private BlockingQueue<String> writeCommands = new LinkedBlockingQueue<>();
    private FakePsyncServer fakePsyncServer;

    public FakePsyncAction(FakePsyncServer fakePsyncServer, Socket socket) {
        super(socket);
        this.fakePsyncServer = fakePsyncServer;
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
        if (Sync.PSYNC.equalsIgnoreCase(args[0])) {
            String replId = args[1];
            long offset = Long.parseLong(args[2]);
            Long rdbOffset = fakePsyncServer.handlePsync(replId, offset);
            try {
                if (null == rdbOffset) {
                    handlePartialSync(ous);
                } else {
                    handleFullSync(replId, offset, ous);
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
        String resp = "+" + Sync.PARTIAL_SYNC + "\r\n";
        ous.write(resp.getBytes());
        ous.flush();

        fakePsyncServer.addCommandListener(this);
        writeCommands(ous);
    }

    private void handleFullSync(String replId, long offset, OutputStream ous) throws IOException, InterruptedException {
        if (replId.equalsIgnoreCase("?")){
            replId = DefaultRunIdGenerator.DEFAULT.generateRunid();
            offset = 666L;
        }
        String resp = String.format("+%s %s %d\r\n", Sync.FULL_SYNC, replId, offset);
        ous.write(resp.getBytes());
        ous.flush();

        fakePsyncServer.addCommandListener(this);
        ous.write(fakePsyncServer.genRdbData());
        ous.flush();

        writeCommands(ous);
    }

    private void writeCommands(OutputStream ous) throws IOException, InterruptedException {

        startReadThread();

        while (true) {

            String command = writeCommands.poll(10, TimeUnit.MILLISECONDS);
            if (getSocket().isClosed()) {
                logger.info("[writeCommands][closed]");
                return;
            }
            if (command == null) {
                continue;
            }
            String[] args = command.trim().split(" ");
            if (0 == args.length) return;

            StringBuilder sb = new StringBuilder();
            sb.append("*").append(args.length).append("\r\n");
            for (String arg : args) {
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
                while (true) {
                    int data = ins.read();
                    if (data == -1) {
                        logger.info("[doRun]read -1, close socket:{}", getSocket());
                        getSocket().close();
                        return;
                    }
                }
            }
        }).start();
    }

    public void addCommands(String command) {
        writeCommands.offer(command);
    }

    @Override
    public void setDead() {
        fakePsyncServer.removeCommandListener(this);
    }

}
