package com.meituan.trip.eswrapper.es.connection;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.springframework.beans.factory.InitializingBean;

import java.util.List;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    :
 */
public class ESConnection implements InitializingBean {
    private TransportClient client;
    private List<InetSocketTransportAddress> transportAddresses;
    private String clusterName;
    private String shieldUserPwd;//用户名密码

    public ESConnection() {
    }

    public ESConnection(List<InetSocketTransportAddress> transportAddresses, String clusterName) {
        this.clusterName = clusterName;
        this.transportAddresses = transportAddresses;
    }

    public ESConnection(TransportClient client, String clusterName,
                        List<InetSocketTransportAddress> transportAddresses, String shieldUserPwd) {
        this.client = client;
        this.clusterName = clusterName;
        this.transportAddresses = transportAddresses;
        this.shieldUserPwd = shieldUserPwd;
    }

    public Client getClient() {
        return client;
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        Settings.Builder builder = Settings.settingsBuilder()
                                           .put("cluster.name", clusterName)
                                           .put("client.transport.sniff", true);
        if (StringUtils.isNotBlank(shieldUserPwd)) {
            builder.put("shield.user", shieldUserPwd);
        }
        Settings settings = builder.build();
        client = TransportClient.builder().settings(settings).build();
        for (TransportAddress transportAddress : transportAddresses) {
            client.addTransportAddress(transportAddress);
        }
    }

}
