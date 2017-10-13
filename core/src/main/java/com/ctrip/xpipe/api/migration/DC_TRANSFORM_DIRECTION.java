package com.ctrip.xpipe.api.migration;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public enum  DC_TRANSFORM_DIRECTION {

    OUTER_TO_INNER,
    INNER_TO_OUTER;

    private static DcMapper dcMapper = DcMapper.INSTANCE;

    public String transform(String dc){

        if(this == OUTER_TO_INNER){
            return dcMapper.reverse(dc);
        }

        if(this == INNER_TO_OUTER){
            return dcMapper.getDc(dc);
        }

        throw new IllegalStateException("unknown diection:" + this);
    }

}
