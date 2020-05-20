package com.ctrip.xpipe.service.metric;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hickwall.proxy.HickwallClientConfig;
import com.ctrip.hickwall.proxy.common.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * May 20, 2020
 */
public class HickwallClient {

    private static final Logger logger = LoggerFactory.getLogger(HickwallClient.class);
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
        String s = JSON.toJSONString(datapoints, new SerializerFeature[]{SerializerFeature.DisableCircularReferenceDetect});
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
                        InputStream in = httpURLConnection.getErrorStream();
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

    public Boolean sendAsync(DataPoint datapoint) {
        if (this.queue == null) {
            synchronized(this) {
                if (this.queue == null) {
                    this.queue = new ArrayBlockingQueue(this.config.BUFFER_SIZE);

                    for(int i = 0; i < this.config.THREAD_NUM; ++i) {
                        Thread t = new Thread() {
                            public void run() {
                                while(!com.ctrip.hickwall.proxy.HickwallClient.this.close || !com.ctrip.hickwall.proxy.HickwallClient.this.queue.isEmpty()) {
                                    ArrayList array = new ArrayList();

                                    try {
                                        DataPoint dp = (DataPoint) com.ctrip.hickwall.proxy.HickwallClient.this.queue.take();

                                        for(Integer c = 0; dp != null && c < com.ctrip.hickwall.proxy.HickwallClient.this.config.BATCH_SIZE; dp = (DataPoint) com.ctrip.hickwall.proxy.HickwallClient.this.queue.poll((long) com.ctrip.hickwall.proxy.HickwallClient.this.config.BUFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                            array.add(dp);
                                            c = c + 1;
                                        }

                                        if (dp != null) {
                                            array.add(dp);
                                        }

                                        Boolean success = false;
                                        Integer reties = com.ctrip.hickwall.proxy.HickwallClient.this.config.RETRIES;

                                        String s;
                                        for(s = JSON.toJSONString(array); !success && reties >= 0; reties = reties - 1) {
                                            try {
                                                success = com.ctrip.hickwall.proxy.HickwallClient.this.send(s);
                                            } catch (Exception var9) {
                                                com.ctrip.hickwall.proxy.HickwallClient.logger.warn("", var9);
                                            }
                                        }

                                        if (!success) {
                                            com.ctrip.hickwall.proxy.HickwallClient.logger.warn("fail to send: " + s);
                                        }
                                    } catch (Exception var10) {
                                        com.ctrip.hickwall.proxy.HickwallClient.logger.warn("", var10);
                                    }
                                }

                            }
                        };
                        t.setDaemon(true);
                        t.start();
                    }
                }
            }
        }

        return this.queue.offer(datapoint);
    }

    public void close() {
        this.close = true;
    }

    protected void finalize() throws Throwable {
        super.finalize();
        this.close();
    }
}
