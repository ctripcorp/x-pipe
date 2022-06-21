package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.web.client.HttpStatusCodeException;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class KeeperContainerErrorParser {
    public static final String ERROR_HEADER_NAME = "X-Keeper-Container-Error";
    private static final Gson gson = new Gson();
    private static final Type errorMessageType = new TypeToken<ErrorMessage<KeeperContainerErrorCode>>(){}.getType();
    private static final Type mapMessageType = new TypeToken<Map<String, String>>(){}.getType();

    public static RuntimeException parseErrorFromHttpException(HttpStatusCodeException ex) {
        if (Objects.requireNonNull(ex.getResponseHeaders()).containsKey(ERROR_HEADER_NAME)) {
            try {
                ErrorMessage<KeeperContainerErrorCode> errorMessage = gson.fromJson(ex.getResponseBodyAsString(),
                        errorMessageType);
                return new KeeperContainerException(errorMessage, ex);
            } catch (Throwable e) {
                //ignore
            }
        }

        try {
            Map<String, String> mapMessage = gson.fromJson(ex.getResponseBodyAsString(), mapMessageType);
            if (mapMessage.containsKey("message")) {
                return new KeeperContainerException(mapMessage.get("message"), ex);
            }
        } catch (Throwable e) {
           //ignore
        }

        return ex;
    }
}
