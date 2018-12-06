package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.utils.ScriptExecutor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public abstract class AbstractScriptExecutor<V> extends AbstractCommand<V> implements ScriptExecutor<V> {

    private static final String SYSTEM = "System";

    private static final String BIN_BASH = "/bin/sh", DASH_C = "-c";

    @Override
    protected void doExecute() throws Exception {
        List<String> lines = getBashCommandInfo(getScript());
        if(lines == null) {
            return;
        }
        future().setSuccess(format(lines));
    }

    private List<String> getBashCommandInfo(String command) {
        String[] cmds = new String[] {BIN_BASH, DASH_C, command};
        return getBashCommandInfo(cmds);
    }

    private List<String> getBashCommandInfo(String[] commands) {
        InputStreamReader sr = null;
        BufferedReader br = null;
        Transaction t = Cat.newTransaction(SYSTEM, "Bash.Command");
        try {
            t.addData(Arrays.toString(commands));

            Process process = new ProcessBuilder().command(commands).start();
            sr = new InputStreamReader(process.getInputStream(), "UTF-8");
            br = new BufferedReader(sr);
            List<String> lines = new ArrayList<>();
            String s;
            while ((s = br.readLine()) != null) {
                lines.add(s);
            }
            return lines;
        } catch (IOException e) {
            t.setStatus(e);
            logger.warn("[getBashCommandInfo]", e);
            future().setFailure(e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (sr != null) {
                    sr.close();
                }
            } catch (IOException e) {
                future().setFailure(e);
            }
            t.complete();
        }
        return null;
    }
}
