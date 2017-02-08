package org.s8sg.connect.zookeeper;


import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

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
	private final List<ZKDataEntry> sync_array_list;
	private final Map<String, Semaphore> nodeLocks;

	public Zookeeperer(List<ZKDataEntry> sync_array_list, Map<String, Semaphore> nodeLocks) {
		this.sync_array_list = sync_array_list;
		this.nodeLocks = nodeLocks;
	}

	public void setZkClient(ZooKeeper zk) {
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			if (event.getType().equals(EventType.NodeDataChanged)) {
				// Unlock of the item for which call back is received
				if (this.nodeLocks.get(event.getPath()).availablePermits() < 1) {
					synchronized (this.nodeLocks.get(event.getPath())) {
						this.nodeLocks.get(event.getPath()).release();
					}
				}
				final Stat stat = new Stat();
				final byte[] data = this.zk.getData(event.getPath(), false, stat);
				final String dataString = new String(data);
				// Add the data string to the sync_array_list
				this.sync_array_list.add(new ZKDataEntry(Integer.toString(stat.hashCode()), event.getPath(), dataString));
			}
		} catch (final Exception e) {
			logger.error("Exception in processing the watch.", e);
		}
	}
}
