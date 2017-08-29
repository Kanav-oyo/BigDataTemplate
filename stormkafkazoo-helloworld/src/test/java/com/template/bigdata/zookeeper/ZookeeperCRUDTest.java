package com.template.bigdata.zookeeper;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Created by arunmohzi on 8/28/17.
 */
public class ZookeeperCRUDTest {
    ZooKeeperLocal zooKeeperLocal; // initiate local zookeeper

    @Mock
    CuratorFramework curatorFramework;

    ZookeeperCRUD zookeeperClientWrapper;

    @BeforeMethod
    public void beforeEachMethod() throws IOException {
        zooKeeperLocal = new ZooKeeperLocal();
        zookeeperClientWrapper = ZookeeperCRUD.getInstance();
        curatorFramework = Mockito.mock(CuratorFramework.class);
        zookeeperClientWrapper.injectCuratorClient(curatorFramework);
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(curatorFramework.checkExists()).thenReturn(existsBuilder);
        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);

    }

    @Test
    public void shouldReadZnodeWhenExist() throws Exception {
        when(curatorFramework.checkExists().forPath(anyString())).thenReturn(new Stat());
        String expectedData = "hello";
        when(curatorFramework.getData().forPath(anyString())).thenReturn(expectedData.getBytes());
        String zNodeData = zookeeperClientWrapper.readZNodeData("/path1/path2");
        assertEquals(zNodeData, expectedData);
    }

    @Test
    public void shouldReturnNullWhenZNodePathNotExist() throws Exception {
        when(curatorFramework.checkExists().forPath(anyString())).thenReturn(null);
        String zNodeData = zookeeperClientWrapper.readZNodeData("/path1/path2");
        verify(curatorFramework, never()).getData();
        assertNull(zNodeData);
    }

    @Test
    public void shouldReturnNullWhenZNodeIsNull() throws Exception {
        when(curatorFramework.checkExists().forPath(anyString())).thenReturn(new Stat());
        when(curatorFramework.getData().forPath(anyString())).thenReturn(null);
        String zNodeData = zookeeperClientWrapper.readZNodeData("/path1/path2");
        assertNull(zNodeData);
    }

}