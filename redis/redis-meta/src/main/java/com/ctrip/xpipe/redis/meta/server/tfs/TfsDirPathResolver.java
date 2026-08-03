package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * Renders ForceCloseDir dir_path from QConfig template.
 */
public final class TfsDirPathResolver {

    private static final String KEEPER_PORT_PLACEHOLDER = "{keeper_port}";

    private static final String REPL_ID_PLACEHOLDER = "{repl_id}";

    private TfsDirPathResolver() {
    }

    /**
     * @param keeperPort keeper Redis port
     * @param replId     shardDbId（与 KeeperTransMeta.replId 一致）
     */
    public static String resolve(String template, int keeperPort, long replId) {
        if (template == null) {
            return "";
        }
        return template
                .replace(KEEPER_PORT_PLACEHOLDER, String.valueOf(keeperPort))
                .replace(REPL_ID_PLACEHOLDER, String.valueOf(replId));
    }
}
