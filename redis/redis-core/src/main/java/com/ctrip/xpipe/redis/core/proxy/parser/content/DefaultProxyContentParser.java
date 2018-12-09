package com.ctrip.xpipe.redis.core.proxy.parser.content;

import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.utils.StringUtil;

import static com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION.CONTENT;

public class DefaultProxyContentParser implements ProxyOptionParser, ProxyContentParser {

    private SubOptionParser subOptionParser;

    private ContentType type;

    @Override
    public String output() {
        return String.format("%s %s", CONTENT.name(), subOptionParser.output());
    }

    @Override
    public ProxyOptionParser read(String option) {
        String[] optionArr = StringUtil.splitRemoveEmpty(AbstractProxyOptionParser.WHITE_SPACE, option);
        subOptionParser = getSubOptionParser(optionArr[1]);
        String[] suboption;
        if(optionArr.length > 2) {
            suboption = new String[optionArr.length - 2];
            System.arraycopy(optionArr, 2, suboption, 0, suboption.length);
        } else {
            suboption = new String[0];
        }
        subOptionParser.parse(suboption);
        return this;
    }

    @Override
    public PROXY_OPTION option() {
        return CONTENT;
    }

    @Override
    public boolean isImportant() {
        return subOptionParser.isImportant();
    }

    public DefaultProxyContentParser setSubOptionParser(SubOptionParser subOptionParser) {
        this.subOptionParser = subOptionParser;
        return this;
    }

    public DefaultProxyContentParser setType(ContentType type) {
        this.type = type;
        return this;
    }

    private SubOptionParser getSubOptionParser(String suboption) {
        type = ContentType.valueOf(suboption);
        switch (type) {
            case COMPRESS:
                return new CompressParser();
        }
        throw new IllegalArgumentException("Unknown content type");
    }

    @Override
    public SubOptionParser getSubOption() {
        return subOptionParser;
    }

    @Override
    public ContentType getContentType() {
        return type;
    }
}
