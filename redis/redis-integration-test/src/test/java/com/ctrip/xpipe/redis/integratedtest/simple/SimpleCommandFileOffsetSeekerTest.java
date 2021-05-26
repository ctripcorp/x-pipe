package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author wenchao.meng
 * Apr 23, 2021
 */
public class SimpleCommandFileOffsetSeekerTest extends AbstractTest {

    private String commandFile1 = "~/tmp/cmd_3512732e-e07a-4b27-912b-26b4561d2450_997506897712_1";
    private String commandFile2 = "~/tmp/cmd_a31a87b5-e581-4021-809a-28da717e20a9_997506665344_2";

    @Test
    public void testCmpFile() {

        long file1Off = extraceOff(commandFile1);
        long file2Off = extraceOff(commandFile2);
        logger.info("{}, {}, diff:{}", file1Off, file2Off, file1Off - file2Off);

//        long file1Begin = 390222698L;
        long file1Begin = 390227024L;
        long file2Begin = file1Off + file1Begin - file2Off;
        if(file2Begin<0){
            logger.error("file2Begin {}", file2Begin);
            return;
        }
        commandFile1 = getRealDir(commandFile1);
        commandFile2 = getRealDir(commandFile2);

        long lenDiff = file2Off - file1Off;
        int equalCount = 0;
        int trunkCount = 1 << 10;
        byte[] data1 = new byte[trunkCount];
        byte[] data2 = new byte[trunkCount];
        boolean equals = true;
        try (
                RandomAccessFile file1 = new RandomAccessFile(commandFile1, "r");
                RandomAccessFile file2 = new RandomAccessFile(commandFile2, "r")) {
            file1.seek(file1Begin);
            file2.seek(file2Begin);
            while (equals) {
                int len1 = file1.read(data1);
                int len2 = file2.read(data2);

                if (len1 != trunkCount || len2 != trunkCount) {
                    logger.info("read bytes length not full:{}, {}, break, total passed:{}", len1, len2, equalCount);
                    break;
                }
                for (int i = 0; i < trunkCount; i++) {
                    if (data1[i] != data2[i]) {
                        logger.info("NotEquals, equalsCount: {}, file1off:{}, file2Off:{}", equalCount, file1Begin + equalCount, file2Begin + equalCount);
                        equals = false;
                        break;
                    }
                    equalCount++;
                    if ((equalCount & (equalCount - 1)) == 0) {
                        logger.info("equalCount: {}", equalCount);
                    }
                }


            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testSeekFile() throws IOException {

        String realDir = getRealDir(commandFile1);
        try (RandomAccessFile file = new RandomAccessFile(realDir, "r")) {
            long seekPos = 390222698L;
            file.seek(seekPos);
            long[] targetPos = new long[]{390227024L, 390242192L};
            int targetIndex = 0;


            long curpos = seekPos;
//        file.seek(390242192L - 100L);
            int length = 20 << 10;
            for (int i = 0; i < length; i++) {
                int read = file.read();
                if (read == -1) {
                    break;
                }
                curpos++;
                if (curpos == targetPos[targetIndex]) {
                    System.out.println("=========target:targetPos[targetIndex]========");
                    targetIndex++;
                }
                System.out.print((char) read);
            }
        }
    }

    private long extraceOff(String commandFile) {
        String[] sp = commandFile.split("_");
        if (sp.length < 3) {
            throw new IllegalArgumentException("can not extrace length from:" + commandFile);
        }
        return Long.parseLong(sp[2]);
    }


}
