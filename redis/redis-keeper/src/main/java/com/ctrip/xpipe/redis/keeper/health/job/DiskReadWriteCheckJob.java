package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class DiskReadWriteCheckJob extends AbstractCommand<Boolean> {

    private String path;

    private static final String TEST_FILE = "foo";

    public DiskReadWriteCheckJob(String path) {
        this.path = path;
    }

    @Override
    protected void doExecute() throws Throwable {
        File dir = new File(path);
        File file = new File(path + File.separator + TEST_FILE);
        String data = String.valueOf(System.currentTimeMillis() / 1000);
        BufferedReader bufferedReader = null;
        FileOutputStream outputStream = null;

        try {
            if (!dir.exists()) dir.mkdirs();

            // check writable
            outputStream = new FileOutputStream(file);
            outputStream.write(data.getBytes());

            // check readable
            bufferedReader = new BufferedReader(new FileReader(file), 16);
            String result = bufferedReader.readLine();
            future().setSuccess(StringUtil.trimEquals(data, result));
        } catch (Throwable th) {
            future().setSuccess(false);
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (Throwable th) {
                    getLogger().warn("[reader close fail]", th);
                }
            }
            if (null != outputStream) {
                try {
                    outputStream.close();
                } catch (Throwable th) {
                    getLogger().warn("[outputStream close fail]", th);
                }
            }
        }
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
