package com.ctrip.xpipe.service.fireman;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Codes;

public class DecodeTest {

    private Logger logger = LoggerFactory.getLogger(DecodeTest.class);

    @Test
    public void testDecode() {
        logger.info("[result] {}", decode("~{b270ef4ce0e3e4a5a5fcf53348b22bc5621664e3f3207252712b3070703db33fb74e8}"));
        logger.info("{}", decode("~{c394f41eb64a7ed42a3c3c76beda3c568a96d64a7e0090a29858f090901cf70de45f9}"));
    }
    private String decode(String src) {
        if (src == null) {
            return null;
        }

        if (src.startsWith("~{") && src.endsWith("}")) {
            try {
                return Codes.forDecode().decode(src.substring(2, src.length() - 1));
            } catch (Exception e) {
                logger.error("Unable to decode value: {}", src, e);
            }
        }

        return src;
    }
}
