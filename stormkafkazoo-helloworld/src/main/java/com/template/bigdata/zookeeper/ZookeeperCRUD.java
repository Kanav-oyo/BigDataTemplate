package com.template.bigdata.zookeeper;

import com.google.common.annotations.VisibleForTesting;
import joptsimple.internal.Strings;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

/**
 *  Sample template to cover CRUD - (create, Read, Update, Delete) persistent znode in a zookeeper
 */
public class ZookeeperCRUD {

    public static ZookeeperCRUD instance;
    private static CuratorFramework curatorClient;

    private ZookeeperCRUD() {
        curatorClient = createCuratorClient("localhost:2181");
        initiateCuratorClient();
    }

    public static ZookeeperCRUD getInstance() {
        if (instance == null) {
            synchronized (ZookeeperCRUD.class) {
                if (instance == null) {
                    instance = new ZookeeperCRUD();
                }
            }
        }
        return instance;
    }

    private static CuratorFramework createCuratorClient(String connectionString) {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    private static void initiateCuratorClient() {
        curatorClient.start();
    }

    /**
     * readZNodeData method accepts znode path and returns znode content.
     * Also, returns null on path/content non existent state.
     * @param path
     * @return
     */
    @VisibleForTesting
    String readZNodeData(String path) {
        //TODO: throw exceptions instead of returning null
        try {
            Stat stat = curatorClient.checkExists().forPath(path);
            if(stat == null) return null; // path does not exist
            String zNodeData =  new String(curatorClient.getData().forPath(path));
            if (Strings.isNullOrEmpty(zNodeData)) return null;
            else return zNodeData;
        } catch (Exception e) {
            //swallow exception
        }
        return null;
    }

    @VisibleForTesting
    void injectCuratorClient(CuratorFramework curatorClient) {
        this.curatorClient = curatorClient;
    }

}
