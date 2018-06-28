package cn.chinaunicom.js.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author : chris
 * 2018-06-28
 */
public class CuratorOperator {
    public CuratorFramework client;
    public static final String ZK_SERVER_PATH = "101.132.47.22:2181";

    public CuratorOperator() {
        // 重试机制
        /*
        同步创建zk实例，原生api是异步的
        curator连接zookeeper策略:ExponentialBackoffRetry
        baseSleepTimeMs --> 初始sleep时间9ªª
        maxRetries --> 最大重试次数
        maxSleepMs --> 最大重试时间
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
