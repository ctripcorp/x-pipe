package com.ctrip.xpipe.zk;

import com.ctrip.xpipe.api.codec.Codec;
import org.apache.curator.framework.recipes.cache.ChildData;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 29, 2017
 */
public class ZkUtils {

    public static String toString(ChildData childData){

        if(childData == null){
            return "null";
        }
        return String.format("[path:%s, stat:%s, data:%s]", childData.getPath(), childData.getStat(), new String(childData.getData(), Codec.defaultCharset));
    }
}
