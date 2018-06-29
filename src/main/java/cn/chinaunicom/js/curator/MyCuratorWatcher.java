package cn.chinaunicom.js.curator;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * @author : WangPingChun
 * 2018-06-29
 */
public class MyCuratorWatcher implements CuratorWatcher {
    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("触发Watcher:" + watchedEvent.getPath());
    }
}
