package com.ctrip.xpipe.simpleserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 16, 2018
 */
public abstract class AbstractIoActionFactory implements IoActionFactory {

    @Override
    public IoAction createIoAction(Socket socket) {

        return new AbstractIoAction(socket) {
            @Override
            protected Object doRead(InputStream ins) throws IOException {
                return _doRead(ins);
            }

            @Override
            protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                byte [] data = getToWrite(readResult);
                ous.write(data);
                ous.flush();
            }
        };
    }

    private Object _doRead(InputStream ins) throws IOException {

        return AbstractIoAction.readLine(ins);

    }

    protected abstract byte[] getToWrite(Object readResult);
}
