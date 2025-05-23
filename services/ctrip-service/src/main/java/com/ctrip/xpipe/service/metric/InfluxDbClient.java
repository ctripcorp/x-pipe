package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/10/26
 */
public class InfluxDbClient implements Closeable {

    private InfluxDB influxDB;

    private final String host;

    private final String db;

    private volatile boolean close = false;

    private static final String TYPE = "Influxdb";

    public InfluxDbClient(String host, String db) {
        this.host = host;
        this.db = db;
    }

    public String getHost() {
        return host;
    }

    public String getDb() {
        return db;
    }

    public org.influxdb.InfluxDB getInfluxDB() {
        if(influxDB == null) {
            this.influxDB = InfluxDBFactory.connect(host);
            this.influxDB.setDatabase(db);
        }
        return influxDB;
    }

    // points should be less then 3000 one time
    public void send(List<Point> points) {
        BatchPoints batchPoints = BatchPoints.database(db).build();
        points.forEach(batchPoints::point);
        getInfluxDB().write(batchPoints);
    }

    public void sendWithMonitor(List<Point> points) throws Exception {
        BatchPoints batchPoints = BatchPoints.database(db).build();
        points.forEach(batchPoints::point);

        TransactionMonitor.DEFAULT.logTransaction(TYPE, "send", new Task<Object>() {
            @Override
            public void go() {
                getInfluxDB().write(batchPoints);
            }

            @Override
            public Map<String, Object> getData() {
                return new HashMap<String, Object>() {{
                   put("points", batchPoints.getPoints().size());
                   put("address", host);
                   put("database", db);
                }};
            }
        });
    }

    public void ping() {
        getInfluxDB().ping();
    }

    @Override
    public String toString() {
        return "InfluxDbClient{" +
                "host='" + host + '\'' +
                ", db='" + db + '\'' +
                '}';
    }

    @Override
    public void close() {
        if(influxDB != null) {
            influxDB.close();
        }
        this.close = true;
    }

    protected void finalize() {
        if (!this.close) this.close();
    }

}
