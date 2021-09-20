package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.core.spring.AbstractWebConfig;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.VelocityException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Properties;

/**
 * @author lishanglin
 * date 2021/5/18
 */
@Configuration
public class ConsoleWebConfig extends AbstractWebConfig {

    static String s = "# CRDT Replication\r\novc:1:10508243557;2:14336563164;3:9436102667\r\ngcvc:1:10508243557;2:14336563164;3:9435065867\r\ngid:3\r\nbackstreaming:0\r\n#Peer_Master_0\r\npeer0_host:10.61.61.142\r\npeer0_port:6379\r\npeer0_gid:1\r\npeer0_dbid:0\r\npeer0_link_status:up\r\npeer0_last_io_seconds_ago:0\r\npeer0_sync_in_progress:0\r\npeer0_repl_offset:4736800345598\r\npeer0_replid:d6d4e7513cc60152552c1ae89b3979a2770a96ab\r\n#Peer_Master_1\r\npeer1_host:10.25.239.57\r\npeer1_port:6379\r\npeer1_gid:2\r\npeer1_dbid:0\r\npeer1_link_status:up\r\npeer1_last_io_seconds_ago:0\r\npeer1_sync_in_progress:0\r\npeer1_repl_offset:15637013029955\r\npeer1_replid:5079b140c50eda4d765d3403ac12b0204c2fd11f\r\nconnected_slaves:3\r\nslave0:ip=10.25.239.57,port=6379,state=online,offset=9791591593411,lag=1\r\nslave1:ip=10.61.61.142,port=6379,state=online,offset=9791591593499,lag=0\r\nslave2:ip=10.61.61.143,port=6379,state=online,offset=9791591593587,lag=0\r\nmaster_replid:f6a5591d99bb99ea4e2e2752c427cb13b80ed4d3\r\nmaster_replid2:849fe7c3c735076f4a0a7e79e8ffbf4ee0d26a31\r\nmaster_repl_offset:9791591593587\r\nsecond_repl_offset:9791591512351\r\nrepl_backlog_active:1\r\nrepl_backlog_size:134217728\r\nrepl_backlog_first_byte_offset:9791457375860\r\nrepl_backlog_histlen:134217728\r\n";
    public static void main(String[] args) {
        System.out.println(s);
    }

    @Bean
    public VelocityEngine getVelocityEngine() throws VelocityException, IOException {
        Properties props = new Properties();
        props.put("resource.loader", "class");
        props.put("class.resource.loader.class",
                "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        return new VelocityEngine(props);
    }

}
