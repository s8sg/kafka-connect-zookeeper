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
	private List<String> sync_array_list;
	private String topic;
	private String zk_hosts;
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final String NODENAME_FIELD = "nodename";
	private static final String POSITION_FIELD = "position";
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
		this.sync_array_list = Collections.synchronizedList(new ArrayList<String>());
		this.zooKeeperer = new Zookeeperer(this.sync_array_list);
		try {
			this.zoo = new ZooKeeper(this.zk_hosts, 1000, this.zooKeeperer);
		} catch (final IOException e) {

		}
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
				this.sync_array_list.add(dataString);
				this.start = false;
			}
		} catch (final KeeperException e) {
			e.printStackTrace();
			return null;
		}

		// Get all the newly added data from the
		synchronized(this.sync_array_list) {
			final Iterator<String> iterator = this.sync_array_list.iterator();
			int pos = 0;
			while (iterator.hasNext()) {
				final String value = iterator.next();
				records.add(new SourceRecord(sourcePartition(), sourceOffset(pos), this.topic, VALUE_SCHEMA, value));
				iterator.remove();
				pos++;
			}
		}

		return records;
	}

	private Map<String, String> sourcePartition() {
		return Collections.singletonMap("host", this.zk_hosts);
	}

	private Map<String, String> sourceOffset(int pos) {
		final Map<String, String> m = new HashMap<String, String>();
		m.put(POSITION_FIELD, Integer.toString(pos));
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