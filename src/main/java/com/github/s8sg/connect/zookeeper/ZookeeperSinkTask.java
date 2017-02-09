package com.github.s8sg.connect.zookeeper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSinkTask extends SinkTask {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperSinkTask.class);
	private String zk_node;
	private String zk_hosts;
	private Zookeeperer zooKeeperer;
	private ZooKeeper zoo;
	private String latestData;
	// As ZooKeeper doesn't need any historical data for a node, only latest data is stored

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> confs) {
		this.zk_node= confs.get(ZookeeperSinkConnector.ZK_NODE);
		this.zk_hosts = confs.get(ZookeeperSinkConnector.ZK_HOSTS);
		this.latestData = null;
		try {
			this.zoo = new ZooKeeper(this.zk_hosts, 1000, this.zooKeeperer);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		for (final SinkRecord record : sinkRecords) {
			this.latestData = record.value().toString();
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
		try {
			final Stat stat = this.zoo.exists(this.zk_hosts, false);
			if (stat != null) {
				this.zoo.setData(this.zk_node, this.latestData.getBytes(), stat.getVersion());
			} else {
				this.zoo.create(this.zk_node, this.latestData.getBytes(),  Ids.OPEN_ACL_UNSAFE,  CreateMode.PERSISTENT);
			}
		} catch(final InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
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
