package cn.chinaunicom.js.config;

import cn.chinaunicom.js.utils.JsonUtils;
import cn.chinaunicom.js.utils.RedisConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author : chris
 * 2018-06-29
 */
public class ClientThree {
    private static final String ZK_SERVER_PATH = "101.132.47.22:2181,101.132.47.22:2181,111.231.115.63:2181";
    private static final String REDIS_CONFIG_PATH = "/o2o";
    private static final String SUB_PATH = "/redis-config";
    public static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static Logger log = LoggerFactory.getLogger(ClientThree.class);
    private CuratorFramework client;

    public ClientThree() {
        RetryPolicy policy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVER_PATH)
                .sessionTimeoutMs(30000)
                .retryPolicy(policy)
                .namespace("workspace")
                .build();
        client.start();
    }

    public static void main(String[] args) throws Exception {
        ClientThree clientThree = new ClientThree();
        log.info("clientThree started!");
        final PathChildrenCache childrenCache = new PathChildrenCache(clientThree.client, REDIS_CONFIG_PATH, true);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        // 添加监听事件
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                final PathChildrenCacheEvent.Type eventType = event.getType();
                if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    String configNodePath = event.getData().getPath();
                    if (configNodePath.endsWith(REDIS_CONFIG_PATH + SUB_PATH)) {
                        log.info("监听到配置发生变化,节点路径为:{}", configNodePath);
                        // 读取节点数据
                        String jsonConfig = new String(event.getData().getData());
                        log.info("节点「{}」的数据为「{}」", configNodePath, jsonConfig);

                        // 从json转换配置
                        RedisConfig redisConfig = null;
                        if (StringUtils.isNotBlank(jsonConfig)) {
                            redisConfig = JsonUtils.jsonToPojo(jsonConfig, RedisConfig.class);
                        }

                        // 配置不为空则进行相应操作
                        if (redisConfig != null) {
                            final String type = redisConfig.getType();
                            final String url = redisConfig.getUrl();
                            final String remark = redisConfig.getRemark();
                            if (StringUtils.equals(type, "add")) {
                                log.info("监听到新增的配置,准备下载");
                                // 连接ftp服务器,根据url找到相应的配置
                                Thread.sleep(1000);
                                log.info("开始下载新的配置文件,下载路径为:{}", url);
                                // 下载配置到你指定的目录
                                Thread.sleep(1000);
                                log.info("下载成功,以添加到项目中");
                                // 拷贝文件到项目目录
                            } else if (StringUtils.equals(type, "update")) {
                                log.info("监听到更新的配置,准备下载");
                                // 连接ftp服务器,根据url找到相应的配置
                                Thread.sleep(1000);
                                log.info("开始下载新的配置文件,下载路径为:{}", url);
                                // 下载配置到你指定的目录
                                Thread.sleep(1000);
                                log.info("下载成功");
                                Thread.sleep(1000);
                                log.info("删除项目中愿配置文件");
                                Thread.sleep(1000);
                                log.info("拷贝配置文件到项目目录");
                                // 拷贝文件到项目目录
                            } else if (StringUtils.equals(type, "delete")) {
                                log.info("监听到需要删除配置文件");
                                log.info("删除项目中的配置文件");
                            }
                            // TODO 视情况统一重启服务
                        }
                    }
                }
            }
        });
        countDownLatch.await();
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
