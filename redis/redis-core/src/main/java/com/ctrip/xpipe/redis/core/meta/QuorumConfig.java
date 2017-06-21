package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class QuorumConfig {

    private int total = 5;
    private int quorum = 3;

    public QuorumConfig(){

    }

    public QuorumConfig(int total, int quorum){
        this.total = total;
        this.quorum = quorum;
    }



    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getQuorum() {
        return quorum;
    }

    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }

    @Override
    public String toString() {
        return String.format("total:%d, quorum:%d", total, quorum);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(total, quorum);
    }

    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof QuorumConfig)){
            return false;
        }

        QuorumConfig quorumConfig = (QuorumConfig) obj;

        return quorumConfig.total == total && quorumConfig.quorum == quorum;
    }
}
