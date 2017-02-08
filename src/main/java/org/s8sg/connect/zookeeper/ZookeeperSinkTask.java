package org.s8sg.connect.zookeeper;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSinkTask extends SinkTask {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperSinkTask.class);

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		//TODO: Create resources like database or api connections here.
	}

	@Override
	public void put(Collection<SinkRecord> collection) {

	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

	}

	@Override
	public void stop() {
		//Close resources here.
	}

}
