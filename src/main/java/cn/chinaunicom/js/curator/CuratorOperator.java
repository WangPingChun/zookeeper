package cn.chinaunicom.js.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @author : chris
 * 2018-06-28
 */
public class CuratorOperator {
    public CuratorFramework client;
    public static final String ZK_SERVER_PATH = "101.132.47.22:2181,101.132.47.22:2181,111.231.115.63:2181";

    public CuratorOperator() {
        /* ========= Curator重连机制 ========= */
        /*
         * 同步创建zk实例，原生api是异步的
         * curator连接zookeeper策略:ExponentialBackoffRetry
         * baseSleepTimeMs  初始sleep时间
         * maxRetries       最大重试次数
         * maxSleepMs       最大重试时间
         */
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);



        /*
         * n                       重试次数
         * sleepMsBetweenRetries:  每次重试间隔时间
         */
        RetryPolicy retryPolicy = new RetryNTimes(4, 5000);


        /*
         * sleepMsBetweenRetay  每次重试间隔时间
         */
//        RetryPolicy retryPolicy = new RetryOneTime(3000);

        /*
         * 永远重试，不推荐使用
         */
//        RetryPolicy retryPolicy = new RetryForever(3000);

        /*
         * maxElapsedTimeMs         最大重试时间
         * sleepMsBetweenRetries    每次重试间隔
         * 重试时间超过maxElapasedTimesMs后，就不再重试
         */
//        RetryPolicy retryPolicy = new RetryUntilElapsed(2000, 3000);
        /* ========= Curator重连机制 ========= */

        client = CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVER_PATH)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace("workspace")
                .build();
        client.start();
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        CuratorOperator operator = new CuratorOperator();
        boolean started = operator.client.isStarted();
        System.out.println("当前客户端状态：" + (started ? "连接中" : "已关闭"));

        String nodePath = "/super";
        // 创建节点
//        byte[] data = "superme".getBytes();
//        operator.client
//                .create()
//                .creatingParentsIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
//                .forPath(nodePath, data);

        // 更新节点数据
//        byte[] newData = "batman".getBytes();
//        operator.client
//                .setData()
//                .withVersion(0)
//                .forPath(nodePath, newData);

        // 删除节点
//        operator.client.delete()
//                .guaranteed()               // 如果删除失败，那么后端还是继续删除，直到成功
//                .deletingChildrenIfNeeded() // 如果有子节点就删除
//                .withVersion(0)
//                .forPath(nodePath);
//


        // 读取节点数据
//        Stat stat = new Stat();
//        byte[] data = operator.client.getData()
//                .storingStatIn(stat)
//                .forPath(nodePath);
//        System.out.println("节点" + nodePath + "的数据为" + new String(data));
//        System.out.println("改后的版本号为：" + stat.getVersion());

        // 查询子节点
//        List<String> childNodes = operator.client.getChildren().forPath(nodePath);
//        for (String childNode : childNodes) {
//            System.out.println(childNode);
//        }

        // 判断节点是否存在,如果不存在则为空
//        Stat stat = operator.client.checkExists().forPath(nodePath);
//        System.out.println(stat);

        // usingWatcher监听只会触发一次
        operator.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);


        Thread.sleep(1000000);
        operator.close();
    }
}
