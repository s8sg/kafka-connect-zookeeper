package com.github.s8sg.connect.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSourceTask extends SourceTask {
	static final Logger logger = LoggerFactory.getLogger(ZookeeperSourceTask.class);
	private ZooKeeper zoo;
	private Zookeeperer zooKeeperer;
	private String[] zk_nodes;
	private List<ZKDataEntry> sync_array_list;
	private String topic;
	private String zk_hosts;
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final String NODENAME_FIELD = "nodename";
	private static final String HASHKEY_FIELD = "hashkey";
	private Map<String, Boolean> firstData;
	private Map<String, Semaphore> nodeLocks;
	// Node Locks are used avoid multiple registration of Watches for a single ZK Node

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> confs) {
		this.topic = confs.get(ZookeeperSourceConnector.TOPIC_CONFIG);
		this.zk_nodes= confs.get(ZookeeperSourceConnector.ZK_NODES).split(",");
		this.zk_hosts = confs.get(ZookeeperSourceConnector.ZK_HOSTS);
		this.sync_array_list = Collections.synchronizedList(new ArrayList<ZKDataEntry>());
		this.nodeLocks = new HashMap<String, Semaphore>();
		for(final String node : this.zk_nodes) {
			this.nodeLocks.put(node, new Semaphore(1));
		}
		this.firstData = new HashMap<String, Boolean>();
		for(final String node : this.zk_nodes) {
			this.firstData.put(node, true);
		}
		this.zooKeeperer = new Zookeeperer(this.sync_array_list, this.nodeLocks);
		try {
			this.zoo = new ZooKeeper(this.zk_hosts, 1000, this.zooKeeperer);
		} catch (final IOException e) {
			e.printStackTrace();
		}
		this.zooKeeperer.setZkClient(this.zoo);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		final List<SourceRecord> records = new ArrayList<>();
		final Stat stat = new Stat();

		// TODO: Support multiple node, This is being done for only one node now
		for (final String node: this.zk_nodes) {
			try {
				if(this.nodeLocks.get(node).tryAcquire(1, TimeUnit.SECONDS)) {
					final byte[] data = this.zoo.getData(node, this.zooKeeperer, stat);
					if (this.firstData.get(node) == true) {
						final String dataString = new String(data);
						// Add the data string to the sync_array_list
						this.sync_array_list.add(new ZKDataEntry(Integer.toString(stat.getVersion()), node, dataString));
						this.firstData.put(node, false);
					}
				}
			} catch (final KeeperException e) {
				// In case of any error release the lock
				if (this.nodeLocks.get(node).availablePermits() < 1){
					this.nodeLocks.get(node).release();
				}
				e.printStackTrace();
				return null;
			}
		}

		// Get all the newly added data from the sync list
		synchronized(this.sync_array_list) {
			final Iterator<ZKDataEntry> iterator = this.sync_array_list.iterator();
			while (iterator.hasNext()) {
				final ZKDataEntry entry = iterator.next();
				records.add(new SourceRecord(sourcePartition(), sourceOffset(entry.getNode(), entry.getVersion()), this.topic, VALUE_SCHEMA, entry.getValue()));
				iterator.remove();
			}
		}
		return records;
	}

	private Map<String, String> sourcePartition() {
		return Collections.singletonMap("host", this.zk_hosts);
	}

	private Map<String, String> sourceOffset(String node, String hash) {
		final Map<String, String> m = new HashMap<String, String>();
		m.put(HASHKEY_FIELD, hash);
		m.put(NODENAME_FIELD, node);
		return m;
	}

	@Override
	public void stop() {
		try {
			this.zoo.close();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}
}