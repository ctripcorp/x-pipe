package com.ctrip.xpipe.api.sso;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public class SsoConfig {

    public static final String excludeRegex = "/api/.*|/health";

    public static boolean stopsso = false;

    public static boolean excludes(String url){

        if(stopsso){
            return  true;
        }
        return Pattern.matches(SsoConfig.excludeRegex, url);
    }

}

