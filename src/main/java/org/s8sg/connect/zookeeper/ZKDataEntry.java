package org.s8sg.connect.zookeeper;

final class ZKDataEntry{

	private final String hash;
	private final String node;
	private final String value;

	public ZKDataEntry(String hash, String node, String value) {
		this.hash = hash;
		this.node = node;
		this.value = value;
	}

	public String getHash() {
		return this.hash;
	}

	public String getValue() {
		return this.value;
	}

	public String getNode() {
		return this.node;
	}
}
