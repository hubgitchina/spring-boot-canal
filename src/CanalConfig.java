package com.huafa.core.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName: CanalConfig
 * @Description: Canal服务配置
 * @Author wangpeng
 * @Date 2019-6-24 15:14
 * @Version 1.0
 */
@Configuration
@ConfigurationProperties(prefix = "canal.client.instances.example")
public class CanalConfig {

    private boolean cluster;

    private String hostName;

    private int port;

    private String destination;

    private String userName;

    private String password;

    private String listenerDb;

    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getListenerDb() {
        return listenerDb;
    }

    public void setListenerDb(String listenerDb) {
        this.listenerDb = listenerDb;
    }
}