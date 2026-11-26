package com.ctrip.xpipe.redis.console.keeperops;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fake MetaServer for {@link KeeperOpsLocalConsoleTest} (dc jq → http://127.0.0.1:9747).
 * JDK only, no build needed (single-file source launch, JDK 11+):
 * <pre>
 * java redis/redis-console/src/test/java/com/ctrip/xpipe/redis/console/keeperops/KeeperOpsFakeMetaServer.java [port] [mode]
 * </pre>
 * Stop: create {@code ${java.io.tmpdir}/keeper-ops-metaserver.stop}, or call {@code /fake/stop}.
 *
 * <p>Behavior is switched at runtime with {@code GET /fake/mode?value=<mode>}; it applies to
 * {@code POST /api/dcmeta/reload} (what switchBM calls) and to the cluster APIs:
 * <ul>
 *     <li>{@code ok-bm} (default): reload → [APPLIED] → SUCCESS</li>
 *     <li>{@code no-change}: reload → [NO_CHANGE] → SUCCESS</li>
 *     <li>{@code not-applied}: reload → [SKIPPED_RECENT_MODIFY] → META_RELOAD_NOT_APPLIED</li>
 *     <li>{@code peer-failed}: reload → [APPLIED, FAILED server 2] → META_ERROR</li>
 *     <li>{@code unsupported}: reload → HTTP 404 (old MetaServer) → META_RELOAD_UNSUPPORTED</li>
 *     <li>{@code slow}: every API sleeps 70s (> 60s reload client read timeout) → META_TIMEOUT</li>
 *     <li>{@code error}: every API answers HTTP 500 → META_HTTP_ERROR</li>
 *     <li>{@code ok-tfs}: getactivekeeper answers TFS KC 106 (manual checks only)</li>
 * </ul>
 * {@code GET /fake/requests} returns the last 100 requests. Stopping this process simulates META_UNREACHABLE. */
public class KeeperOpsFakeMetaServer {

    public static final File STOP_FILE = new File(System.getProperty("java.io.tmpdir"), "keeper-ops-metaserver.stop");

    private static final String ACTIVE_BM =
            "{\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"ip\":\"127.0.0.1\",\"port\":6001,\"active\":true,\"keeperContainerId\":100}";
    private static final String ACTIVE_TFS =
            "{\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"ip\":\"127.0.0.1\",\"port\":6000,\"active\":true,\"keeperContainerId\":106}";

    private static final long SLOW_MILLI = 70_000;

    private volatile String mode = "ok-bm";
    private volatile boolean stopped = false;
    private final Deque<String> requests = new ArrayDeque<>();

    public static void main(String[] args) throws Exception {
        KeeperOpsFakeMetaServer server = new KeeperOpsFakeMetaServer();
        if (args.length > 1) {
            server.mode = args[1];
        }
        server.start(args.length > 0 ? Integer.parseInt(args[0]) : 9747);
    }

    private void start(int port) throws Exception {
        STOP_FILE.delete();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        server.setExecutor(executor);
        server.createContext("/fake/mode", ex -> {
            String value = queryParam(ex.getRequestURI(), "value");
            if (value != null && !value.isEmpty()) {
                mode = value;
            }
            reply(ex, 200, "mode=" + mode);
        });
        server.createContext("/fake/requests", ex -> {
            synchronized (requests) {
                reply(ex, 200, String.join("\n", requests));
            }
        });
        server.createContext("/fake/stop", ex -> {
            stopped = true;
            reply(ex, 200, "stopping");
        });
        server.createContext("/api/meta/", this::handleMeta);
        server.createContext("/api/dcmeta/reload", this::handleReload);
        server.start();
        System.out.println("[FakeMetaServer] listening on 127.0.0.1:" + port + ", mode=" + mode
                + ", stop file " + STOP_FILE.getAbsolutePath());

        while (!stopped && !STOP_FILE.exists()) {
            Thread.sleep(500);
        }
        STOP_FILE.delete();
        server.stop(0);
        executor.shutdownNow();
    }

    private void handleReload(HttpExchange ex) throws IOException {
        String current = mode;
        record(ex.getRequestMethod() + " " + ex.getRequestURI() + " mode=" + current);
        String self = "{\"serverId\":1,\"address\":\"127.0.0.1:" + ex.getLocalAddress().getPort() + "\",\"status\":\"%s\"%s}";
        switch (current) {
            case "slow":
                try {
                    Thread.sleep(SLOW_MILLI);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                reply(ex, 200, "[" + String.format(self, "APPLIED", "") + "]");
                return;
            case "error":
                reply(ex, 500, "fake metaserver error");
                return;
            case "unsupported":
                reply(ex, 404, "Not Found");
                return;
            case "no-change":
                reply(ex, 200, "[" + String.format(self, "NO_CHANGE", "") + "]");
                return;
            case "not-applied":
                reply(ex, 200, "[" + String.format(self, "SKIPPED_RECENT_MODIFY", "") + "]");
                return;
            case "peer-failed":
                reply(ex, 200, "[" + String.format(self, "APPLIED", "")
                        + ",{\"serverId\":2,\"address\":\"127.0.0.2:8080\",\"status\":\"FAILED\",\"message\":\"getDcMeta Overflow\"}]");
                return;
            default:
                reply(ex, 200, "[" + String.format(self, "APPLIED", "") + "]");
        }
    }

    private void handleMeta(HttpExchange ex) throws IOException {
        String current = mode;
        String path = ex.getRequestURI().getPath();
        record(ex.getRequestMethod() + " " + path + " mode=" + current);

        if ("slow".equals(current)) {
            try {
                Thread.sleep(SLOW_MILLI);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if ("error".equals(current)) {
            reply(ex, 500, "fake metaserver error");
            return;
        }
        if (path.startsWith("/api/meta/getactivekeeper/")) {
            reply(ex, 200, "ok-tfs".equals(current) ? ACTIVE_TFS : ACTIVE_BM);
            return;
        }
        // clusterchange and anything else: accept
        reply(ex, 200, "");
    }

    private void record(String line) {
        String entry = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " " + line;
        System.out.println("[FakeMetaServer] " + entry);
        synchronized (requests) {
            requests.addLast(entry);
            while (requests.size() > 100) {
                requests.removeFirst();
            }
        }
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0 && pair.substring(0, idx).equals(name)) {
                return pair.substring(idx + 1);
            }
        }
        return null;
    }

    private static void reply(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
        ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        }
        ex.close();
    }
}
