package org.s8sg.connect.zookeeper;

import java.util.Map.Entry;

final class ZKDataEntry<K, V> implements Entry<K, V> {

	private final K key;
	private V value;

	public ZKDataEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return this.key;
	}

	@Override
	public V getValue() {
		return this.value;
	}

	@Override
	public V setValue(V value) {
		final V old = this.value;
		this.value = value;
		return old;
	}
}
