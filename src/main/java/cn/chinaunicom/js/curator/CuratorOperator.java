package cn.chinaunicom.js.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;

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
//        boolean started = operator.client.isStarted();
//        System.out.println("当前客户端状态：" + (started ? "连接中" : "已关闭"));

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

        // usingWatcher监听只会触发一次,监听完毕后就销毁
//        operator.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);

        // NodeCache:监听数据节点的变更,会触发时间
//        final NodeCache nodeCache = new NodeCache(operator.client, nodePath);
        // buildIntial:初始化的时候获取node的值并缓存到本地
//        nodeCache.start(true);
//        if (nodeCache.getCurrentData() != null) {
//            System.out.println("节点初始化数据为:" + new String(nodeCache.getCurrentData().getData()) + "!");
//        } else {
//            System.out.println("节点初始化数据为空!");
//        }
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//            @Override
//            public void nodeChanged() throws Exception {
//                String data = new String(nodeCache.getCurrentData().getData());
//                System.out.println("节点路径:" + nodeCache.getPath() + "|数据:" + data);
//            }
//        });

        // 为子节点添加watch
        String childNodePathCache = nodePath;
        // PathChildrenCache:监听数据即诶单的增删改,会触发事件
        // cacheData:设置缓存节点的数据状态
        final PathChildrenCache childrenCache = new PathChildrenCache(operator.client, childNodePathCache, true);
        /*
         * StartMode:初始化方式
         * POST_INITIALIZED_EVENT:异步初始化，初始化之后会触发事件
         * NORMAL:异步初始化
         * BUILD_INITIAL_CACHE:同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
//        Thread.sleep(1000);
//        final List<ChildData> childDataList = childrenCache.getCurrentData();
//        System.out.println("当前数据节点的子节点数据列表:");
//        for (ChildData childData : childDataList) {
//            System.out.println(new String(childData.getData()));
//        }
        // 绑定事件
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) {
                final PathChildrenCacheEvent.Type type = event.getType();
                if (type.equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                    System.out.println("子节点初始化ok");
                } else if (type.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    System.out.println("添加子节点:" + event.getData().getPath());
                    System.out.println("子节点数据:" + new String(event.getData().getData()));
                } else if (type.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    System.out.println("删除子节点:" + event.getData().getPath());
                } else if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("修改子节点路径:" + event.getData().getPath());
                    System.out.println("修改子节点数据:" + new String(event.getData().getData()));
                }

            }
        });

        Thread.sleep(1000000);
        operator.close();
    }
}
