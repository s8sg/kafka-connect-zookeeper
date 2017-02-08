package com.s8sg;

import org.junit.Test;
import org.s8sg.connect.zookeeper.ZookeeperSourceConnectorConfig;

public class MySourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(ZookeeperSourceConnectorConfig.conf().toRst());
  }
}