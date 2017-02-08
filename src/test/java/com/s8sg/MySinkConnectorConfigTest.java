package com.s8sg;

import org.junit.Test;
import org.s8sg.connect.zookeeper.ZookeeperSinkConnectorConfig;

public class MySinkConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(ZookeeperSinkConnectorConfig.conf().toRst());
  }
}
