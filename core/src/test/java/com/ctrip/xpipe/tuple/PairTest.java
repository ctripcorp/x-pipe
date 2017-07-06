package com.ctrip.xpipe.tuple;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.codec.Person;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 06, 2017
 */
public class PairTest extends AbstractTest{

    @Test
    public void testSerial(){

        Pair<Boolean, String> pair = new Pair<>(true, "hello");

        String encode = JsonCodec.INSTANCE.encode(pair);

        logger.info("{}", encode);

        Pair<Boolean, String> decode = JsonCodec.INSTANCE.decode(encode, new GenericTypeReference<Pair<Boolean, String>>() {});
        Assert.assertEquals(pair, decode);
    }

    @Test
    public void testValueObject(){

        Pair<Boolean, Person> pair = new Pair<>(true, new Person(Person.SEX.FEMALE, 100));

        String encode = JsonCodec.INSTANCE.encode(pair);

        logger.info("{}", encode);
        Pair<Boolean, Person> decode = JsonCodec.INSTANCE.decode(encode, new GenericTypeReference<Pair<Boolean, Person>>() {});
        Assert.assertEquals(pair, decode);
    }

    @Test
    public void testMap(){

        Map<String, Person>  map = new HashMap<>();
        map.put("a", new Person(Person.SEX.FEMALE));

        String encode = JsonCodec.INSTANCE.encode(map);

        Map<String, Person> decode = JsonCodec.INSTANCE.decode(encode, new GenericTypeReference<Map<String, Person>>() {});

        logger.info("{}", decode);
        Assert.assertEquals(map, decode);
    }
}
