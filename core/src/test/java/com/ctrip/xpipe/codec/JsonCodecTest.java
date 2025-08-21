package com.ctrip.xpipe.codec;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.OuterClientService.MigrationPublishResult;
import org.junit.Test;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Oct 25, 2016
 */
public class JsonCodecTest extends AbstractTest {

    @Test
    public void testLeftRight() {

        LeftRight leftRight = new LeftRight("left", "right");
        logger.info("{}", JsonCodec.INSTANCE.encode(leftRight));
    }

    @Test
    public void testDeserial() {

        Map<String, String> hashMap = new HashMap<>();

        hashMap.put("key", "value");

        String encode = JsonCodec.INSTANCE.encode(hashMap);
        logger.info("{}", encode);

        Map decode = JsonCodec.INSTANCE.decode(encode, Map.class);
        logger.info("{}, {}", decode.getClass(), decode);


    }

    public static class LeftRight implements Map.Entry<String, String> {

        private String left;
        private String right;

        public LeftRight() {

        }

        public LeftRight(String left, String right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String getKey() {
            return left;
        }

        @Override
        public String getValue() {
            return right;
        }

        @Override
        public String setValue(String value) {
            return right;
        }

        public void setKey(String key) {
            this.left = key;
        }
    }


    @Test
    public void test() {

        JsonCodec jsonCodec = new JsonCodec();

        System.out.println(jsonCodec.encode("123\n345"));
    }

    @Test
    public void decodeWithCapital() {

        MigrationPublishResult res = new MigrationPublishResult();
        res.setMessage("test success");
        res.setSuccess(true);
        System.out.println(Codec.DEFAULT.encode(res));
        System.out.println(Codec.DEFAULT.decode("{\"Success\":true,\"Message\":\"设置成功\"}", MigrationPublishResult.class));
        System.out.println(Codec.DEFAULT.decode("{\"success\":true,\"message\":\"test\"}", MigrationPublishResult.class));
    }

    @Test
    public void testMap() {

        Map decode = JsonCodec.INSTANCE.decode("{\"a\":\"1\"}", Map.class);
        logger.info("{}", decode);

        Map<String, String> data = new HashMap();
        data.put("xpipe.sh3.ctripcorp.com", "SHAOY");
        data.put("xpipe.sh2.ctripcorp.com", "SHAJQ");

        logger.info("{}", JsonCodec.INSTANCE.encode(data));
    }

    @Test
    public void testSet() {

        Set<String> set = new HashSet<>();
        set.add("1");

        logger.info("{}", JsonCodec.INSTANCE.encode(set));
    }

    @Test
    public void testEnum() {

        Person person = new Person(Person.SEX.FEMALE);

        logger.info("{}", JsonCodec.INSTANCE.encode(person));
    }

    @Test
    public void testBoolean(){

        logger.info("{}", JsonCodec.INSTANCE.encode(true));
        logger.info("{}", JsonCodec.INSTANCE.decode("\"true\"", Boolean.class));
    }

    @Test
    public void testString(){

        logger.info("{}", JsonCodec.INSTANCE.encode("nihao"));
        logger.info("{}", JsonCodec.INSTANCE.decode("\"buhao\"", String.class));
    }

}
