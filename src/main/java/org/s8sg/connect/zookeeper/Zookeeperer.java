package org.s8sg.connect.zookeeper;


import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Zookeeperer implements Watcher {
	final static Logger logger = LoggerFactory.getLogger(Zookeeperer.class);
	private ZooKeeper zk;
	private final List<ZKDataEntry<String,String>> sync_array_list;

	public Zookeeperer(List<ZKDataEntry<String,String>> sync_array_list) {
		this.sync_array_list = sync_array_list;
	}

	public void setZkClient(ZooKeeper zk) {
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			if (event.getType().equals(EventType.NodeDataChanged)) {
				final Stat stat = new Stat();
				final byte[] data = this.zk.getData(event.getPath(), false, stat);
				final String dataString = new String(data);
				logger.info("ZK Watcher Callback received data: " + dataString + ", adding to sync_list");
				// Add the data string to the sync_array_list
				this.sync_array_list.add(new ZKDataEntry<String, String>(Integer.toString(stat.hashCode()), dataString));
			}
		} catch (final Exception e) {
			logger.error("Exception in processing the watch.", e);
		}
	}
}
