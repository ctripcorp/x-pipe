package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class MathUtil {


    public static int sum(int ...data){

        int result = 0;

        for(int item : data){

            if(item > 0){
                if(result > Integer.MAX_VALUE - item){
                    throw new IllegalArgumentException("sum bigger than max int");
                }
            }
            if(item < 0){
                if(result < Integer.MIN_VALUE - item){
                    throw new IllegalArgumentException("sum smaller than max int");
                }
            }
            result += item;
        }
        return result;
    }
}
