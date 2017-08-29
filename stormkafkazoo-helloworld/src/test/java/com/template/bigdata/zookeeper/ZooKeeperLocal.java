package com.template.bigdata.zookeeper;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by arunmohzi on 8/28/17.
 */
public class ZooKeeperLocal {

    ZooKeeperServerMain zooKeeperServer;

    public ZooKeeperLocal() throws IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(getZookeeperProperties());
        } catch(Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }


    private static Properties getZookeeperProperties() {
        Properties properties = new Properties();
        properties.put("clientPort", 2181 + "");
        properties.put("dataDir", "/tmp/test/zookeeper");
        return properties;
    }
}
