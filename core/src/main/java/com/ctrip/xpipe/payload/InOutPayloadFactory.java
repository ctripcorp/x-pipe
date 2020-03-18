package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.api.payload.InOutPayload;

public interface InOutPayloadFactory {
    InOutPayload create();

    class DirectByteBufInOutPayloadFactory implements InOutPayloadFactory {

        @Override
        public InOutPayload create() {
            return new DirectByteBufInStringOutPayload();
        }
    }
}
