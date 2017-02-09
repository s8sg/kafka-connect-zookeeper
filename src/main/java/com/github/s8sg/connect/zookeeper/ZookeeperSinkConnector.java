package com.github.s8sg.connect.zookeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSinkConnector extends SinkConnector {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperSinkConnector.class);
	public static final String ZK_HOSTS = "zk-hosts";
	public static final String ZK_NODE = "zk-node";

	private String zk_hosts;
	private String zk_node;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.zk_hosts = props.get(ZK_HOSTS);
		if ((this.zk_hosts == null) || this.zk_hosts.isEmpty()) {
			throw new ConnectException("FileStreamSourceConnector configuration must include 'zk-hosts' setting");
		}
		this.zk_node = props.get(ZK_NODE);
		if ((this.zk_node == null) || this.zk_node.isEmpty()) {
			throw new ConnectException("FileStreamSourceConnector configuration must include 'zk-node' setting");
		}
		if (this.zk_node.contains(",")) {
			throw new ConnectException("FileStreamSourceConnector should only have a single node.");
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ZookeeperSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		final ArrayList<Map<String, String>> configs = new ArrayList<>();
		// Only one input stream makes sense.
		final Map<String, String> config = new HashMap<>();
		config.put(ZK_NODE, this.zk_node);
		config.put(ZK_HOSTS, this.zk_hosts);
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		//TODO: Do things that are necessary to stop your connector.
	}

	@Override
	public ConfigDef config() {
		return ZookeeperSinkConnectorConfig.conf();
	}
}
