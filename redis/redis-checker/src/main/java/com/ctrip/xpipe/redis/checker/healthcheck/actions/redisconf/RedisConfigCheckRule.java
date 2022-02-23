package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

public class RedisConfigCheckRule {

    public static final String CONFIG_CHECK_NAME = "configCheckName";

    public static final String EXPECTED_VAULE = "expectedVaule";

    private String checkType;

    private String configCheckName;

    private String expectedVaule;

    public RedisConfigCheckRule(String checkType, String configCheckName, String expectedVaule) {
        this.checkType = checkType;
        this.configCheckName = configCheckName;
        this.expectedVaule = expectedVaule;
    }

    public String getConfigCheckName() {
        return configCheckName;
    }

    public void setConfigCheckName(String configCheckName) {
        this.configCheckName = configCheckName;
    }

    public String getExpectedVaule() {
        return expectedVaule;
    }

    public void setExpectedVaule(String expectedVaule) {
        this.expectedVaule = expectedVaule;
    }

    public String getCheckType() {
        return checkType;
    }

    public void setCheckType(String checkType) {
        this.checkType = checkType;
    }

    @Override
    public String toString() {
        return "RedisConfigCheckRule{" +
                "checkType='" + checkType + '\'' +
                ", configCheckName='" + configCheckName + '\'' +
                ", expectedVaule='" + expectedVaule + '\'' +

                '}';
    }
}
