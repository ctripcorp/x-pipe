package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.api.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author chen.zhu
 * <p>
 * May 20, 2020
 */
public class HickwallClient implements Closeable {
    static final Logger logger = LoggerFactory.getLogger(HickwallClient.class);
    private List<String> address;
    private int index;
    private volatile ArrayBlockingQueue<DataPoint> queue;
    private volatile boolean close;
    private HickwallClientConfig config;

    public HickwallClient(String address) throws IOException {
        this(address, 1000, 1000);
    }

    public HickwallClient(String address, int connectTimeOut, int readTimeOut) throws IOException {
        this.index = 0;
        this.close = false;
        this.config = new HickwallClientConfig(address);
        this.config.CONNECTION_TIMEOUT_MS = connectTimeOut;
        this.config.READ_TIMEOUT_MS = readTimeOut;
        this.address = Arrays.asList(this.config.PROXY_ADDRESS.split(","));
        Collections.shuffle(this.address);
    }

    public HickwallClient(HickwallClientConfig config) throws IOException {
        this.index = 0;
        this.close = false;
        this.config = config;
        this.address = Arrays.asList(config.PROXY_ADDRESS.split(","));
        Collections.shuffle(this.address);
    }

    private HttpURLConnection getConnection() throws IOException {
        int i = 0;

        while(i <= this.address.size()) {
            try {
                ++this.index;
                if (this.index == this.address.size()) {
                    this.index = 0;
                }

                return this.connect(this.index);
            } catch (IOException var3) {
                ++i;
            }
        }

        throw new IOException("unavailable proxy");
    }

    private HttpURLConnection connect(int index) throws IOException {
        URL url = new URL("http://" + (String)this.address.get(index));
        HttpURLConnection httpURLConnection = (HttpURLConnection)url.openConnection();
        httpURLConnection.setDoInput(true);
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setRequestProperty("charset", "utf-8");
        httpURLConnection.setConnectTimeout(this.config.CONNECTION_TIMEOUT_MS);
        httpURLConnection.setReadTimeout(this.config.READ_TIMEOUT_MS);
        httpURLConnection.setUseCaches(false);
        httpURLConnection.connect();
        return httpURLConnection;
    }

    public Boolean send(ArrayList<DataPoint> datapoints) throws IOException {
        String s = Codec.DEFAULT.encode(datapoints);
        return this.send(s);
    }

    private synchronized Boolean send(String s) throws IOException {
        HttpURLConnection httpURLConnection = this.getConnection();
        boolean var10 = false;

        Boolean var5;
        InputStream in;
        label99: {
            try {
                var10 = true;
                OutputStream out = httpURLConnection.getOutputStream();
                out.write(s.getBytes("UTF-8"));
                out.flush();
                out.close();
                int code = httpURLConnection.getResponseCode();
                if (code == 200) {
                    var5 = true;
                    var10 = false;
                    break label99;
                }

                logger.warn("error " + code);
                var5 = false;
                var10 = false;
            } finally {
                if (var10) {
                    if (httpURLConnection != null) {
                        in = httpURLConnection.getErrorStream();
                        if (in != null) {
                            in.close();
                        }

                        httpURLConnection.disconnect();
                    }

                }
            }

            if (httpURLConnection != null) {
                in = httpURLConnection.getErrorStream();
                if (in != null) {
                    in.close();
                }

                httpURLConnection.disconnect();
            }

            return var5;
        }

        if (httpURLConnection != null) {
            in = httpURLConnection.getErrorStream();
            if (in != null) {
                in.close();
            }

            httpURLConnection.disconnect();
        }

        return var5;
    }



    public void close() {
        this.close = true;
    }

    protected void finalize() throws Throwable {
        super.finalize();
        this.close();
    }
}
