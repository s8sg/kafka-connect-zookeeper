package org.s8sg.connect.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private String zk_node;
	private List<ZKDataEntry<String, String>> sync_array_list;
	private String topic;
	private String zk_hosts;
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final String NODENAME_FIELD = "nodename";
	private static final String HASHKEY_FIELD = "hashkey";
	private boolean start = true;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> confs) {
		this.topic = confs.get(ZookeeperSourceConnector.TOPIC_CONFIG);
		this.zk_node= confs.get(ZookeeperSourceConnector.ZK_NODE);
		this.zk_hosts = confs.get(ZookeeperSourceConnector.ZK_HOSTS);
		this.sync_array_list = Collections.synchronizedList(new ArrayList<ZKDataEntry<String,String>>());
		this.zooKeeperer = new Zookeeperer(this.sync_array_list);
		try {
			this.zoo = new ZooKeeper(this.zk_hosts, 1000, this.zooKeeperer);
		} catch (final IOException e) {
			// TODO: DO something if Connect Fails
		}
		this.zooKeeperer.setZkClient(this.zoo);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		final List<SourceRecord> records = new ArrayList<>();
		final Stat stat = new Stat();
		try {
			final byte[] data = this.zoo.getData(this.zk_node, this.zooKeeperer, stat);
			if (this.start == true) {
				final String dataString = new String(data);
				// Add the data string to the sync_array_list
				this.sync_array_list.add(new ZKDataEntry<String, String>(Integer.toString(stat.hashCode()), dataString));
				this.start = false;
			}
		} catch (final KeeperException e) {
			e.printStackTrace();
			return null;
		}

		// Get all the newly added data from the
		synchronized(this.sync_array_list) {
			final Iterator<ZKDataEntry<String, String>> iterator = this.sync_array_list.iterator();
			while (iterator.hasNext()) {
				final ZKDataEntry<String, String> entry = iterator.next();
				logger.info("Sending value: " + entry.getValue());
				records.add(new SourceRecord(sourcePartition(), sourceOffset(entry.getKey()), this.topic, VALUE_SCHEMA, entry.getValue()));
				iterator.remove();
				logger.info("After remove array list: " + this.sync_array_list);

			}
		}

		return records;
	}

	private Map<String, String> sourcePartition() {
		return Collections.singletonMap("host", this.zk_hosts);
	}

	private Map<String, String> sourceOffset(String hash) {
		final Map<String, String> m = new HashMap<String, String>();
		m.put(HASHKEY_FIELD, hash);
		m.put(NODENAME_FIELD, this.zk_node);
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