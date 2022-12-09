package com.ctrip.xpipe.redis.core.keeper.applier.container;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.web.client.HttpStatusCodeException;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * @author ayq
 * <p>
 * 2022/4/6 16:13
 */
public class ApplierContainerErrorParser {

    public static final String ERROR_HEADER_NAME = "X-Applier-Container-Error";
    private static final Gson gson = new Gson();
    private static final Type errorMessageType = new TypeToken<ErrorMessage<ApplierContainerErrorCode>>() {}.getType();
    private static final Type mapMessageType = new TypeToken<Map<String, String>>() {}.getType();

    public static RuntimeException parseErrorFromHttpException(HttpStatusCodeException ex) {
        if (ex.getResponseHeaders().containsKey(ERROR_HEADER_NAME)) {
            try {
                ErrorMessage<ApplierContainerErrorCode> errorMessage = gson.fromJson(ex.getResponseBodyAsString(),
                        errorMessageType);
                return new ApplierContainerException(errorMessage, ex);
            } catch (Throwable e) {
                //ignore
            }
        }

        try {
            Map<String, String> mapMessage = gson.fromJson(ex.getResponseBodyAsString(), mapMessageType);
            if (mapMessage.containsKey("message")) {
                return new ApplierContainerException(mapMessage.get("message"), ex);
            }
        } catch (Throwable e) {
            //ignore
        }

        return ex;
    }
}

