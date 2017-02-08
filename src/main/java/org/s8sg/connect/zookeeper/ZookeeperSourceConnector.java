package org.s8sg.connect.zookeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSourceConnector extends SourceConnector {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperSourceConnector.class);
	public static final String TOPIC_CONFIG = "topic";
	public static final String ZK_HOSTS = "zk-hosts";
	public static final String ZK_NODES = "zk-nodes";

	private String zk_hosts;
	private String[] zk_nodes;
	private String topic;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		//this.config = new ZookeeperSourceConnectorConfig(props);
		this.zk_hosts = props.get(ZK_HOSTS);
		if ((this.zk_hosts == null) || this.zk_hosts.isEmpty()) {
			throw new ConnectException("FileStreamSourceConnector configuration must include 'zk-hosts' setting");
		}
		this.zk_nodes = props.get(ZK_NODES).split(",");
		if ((this.zk_nodes == null) || (this.zk_nodes.length == 0)) {
			throw new ConnectException("FileStreamSourceConnector configuration must include 'zk-node' setting");
		}
		this.topic = props.get(TOPIC_CONFIG);
		if ((this.topic == null) || this.topic.isEmpty()) {
			throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
		}
		if (this.topic.contains(",")) {
			throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ZookeeperSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int i) {
		final ArrayList<Map<String, String>> configs = new ArrayList<>();
		// Only one input stream makes sense.
		final Map<String, String> config = new HashMap<>();
		config.put(ZK_NODES, String.join(",", this.zk_nodes));
		config.put(ZK_HOSTS, this.zk_hosts);
		config.put(TOPIC_CONFIG, this.topic);
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		//TODO: Do things that are necessary to stop your connector.
	}

	@Override
	public ConfigDef config() {
		return ZookeeperSourceConnectorConfig.conf();
	}
}
