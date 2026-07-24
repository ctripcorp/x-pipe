package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * Renders ForceCloseDir dir_path from QConfig template.
 */
public final class TfsDirPathResolver {

    private static final String KEEPER_PORT_PLACEHOLDER = "{keeper_port}";

    private TfsDirPathResolver() {
    }

    public static String resolve(String template, int keeperPort) {
        if (template == null) {
            return "";
        }
        return template.replace(KEEPER_PORT_PLACEHOLDER, String.valueOf(keeperPort));
    }
}
