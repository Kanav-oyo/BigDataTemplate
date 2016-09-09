package com.stormlearn.chap2;


import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by arunmohzi on 9/6/16.
 */


public class TestRepeatVisitBolt {

    @Mock
    private Jedis jedis;

    @Mock
    private OutputCollector collector;

    @Mock
    private Tuple input;

    @Captor
    private ArgumentCaptor<Values> valuesCaptor;

    private RepeatVisitBolt bolt;

    @Before
    public void setup() {

        MockitoAnnotations.initMocks(this);

        bolt = new RepeatVisitBolt();

        bolt.jedis = jedis;
        bolt.collector = collector;

    }

//    private Answer<String> getRedisDBoutput = new Answer<String>() {
//        @Override
//        public String answer(InvocationOnMock invocationOnMock) throws Throwable {
//            Object[] args = invocationOnMock.getArguments();
//            String key = (String) args[1];
//            String value = null;
//            switch (key) {
//                case "Client2:192.168.33.101":
//                    value = "someval";
//                    break;
//                default:
//                    value = null;
//                    break;
//            }
//            return value;
//        }
//    };


    @Test
    public void urlFoundInCache() {

        String ip = "192.168.33.100";
        String url = "myintranet.com";
        String clientKey = "Client1";
        String jedisKey = url + ":" + clientKey;
        String expected = "expected";

        when(input.getStringByField(Chap2Fields.IP)).thenReturn(ip);
        when(input.getStringByField(Chap2Fields.URL)).thenReturn(url);
        when(input.getStringByField(Chap2Fields.CLIENT_KEY)).thenReturn(clientKey);

        when(jedis.get(jedisKey)).thenReturn(expected);

        bolt.execute(input);

        verify(collector).emit(valuesCaptor.capture());
        Values actualValue = valuesCaptor.getValue();
        assertEquals(ImmutableList.of(clientKey, url, Boolean.FALSE.toString()), actualValue);
        verify(jedis).get(jedisKey);
        verify(jedis, never()).set(anyString(), anyString());
    }

    @Test
    public void putsUrlToCacheInFirstVisitOnly() {

        String ip = "192.168.33.100";
        String url = "myintranet.com";
        String clientKey = "Client1";
        String jedisKey = url + ":" + clientKey;
        String expected = null;

        when(input.getStringByField(Chap2Fields.IP)).thenReturn(ip);
        when(input.getStringByField(Chap2Fields.URL)).thenReturn(url);
        when(input.getStringByField(Chap2Fields.CLIENT_KEY)).thenReturn(clientKey);

        when(jedis.get(jedisKey)).thenReturn(expected);

        bolt.execute(input);

        verify(collector).emit(valuesCaptor.capture());
        Values actualValue = valuesCaptor.getValue();
        assertEquals(ImmutableList.of(clientKey, url, Boolean.TRUE.toString()), actualValue);
        verify(jedis).set(jedisKey, "visited");
    }


    public Collection<Object[]> getTestData() {
        Object[][] data = new Object[][]{
                {"192.168.33.100", "Client1", "myintranet.com", "false"},
                {"192.168.33.100", "Client1", "myintranet.com", "false"},
                {"192.168.33.101", "Client2", "myintranet1.com", "true"},
                {"192.168.33.102", "Client3", "myintranet2.com", "false"}};
        return Arrays.asList(data);
    }
}
