package com.ctrip.xpipe.api.sso;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public interface SsoConfig {

    String excludeRegex = "/api/.*|/health";

    static boolean matches(String url){
        return Pattern.matches(SsoConfig.excludeRegex, url);
    }

}

