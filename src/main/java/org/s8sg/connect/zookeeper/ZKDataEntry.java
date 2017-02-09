package org.s8sg.connect.zookeeper;

final class ZKDataEntry{

	private final String version;
	private final String node;
	private final String value;

	public ZKDataEntry(String version, String node, String value) {
		this.version = version;
		this.node = node;
		this.value = value;
	}

	public String getVersion() {
		return this.version;
	}

	public String getValue() {
		return this.value;
	}

	public String getNode() {
		return this.node;
	}
}
