package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.BasicRegionConfig;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

public class ClientClusterManagementServiceDunitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(4);

  private static MemberVM locator, server, serverWithGroupA;
  private static ClientVM client;

  private static String groupA = "group-a";
  private static ClusterManagementService cmsClient;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    serverWithGroupA = cluster.startServerVM(2, groupA, locator.getPort());
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort());
  }

  @Test
  public void createRegion() {
    BasicRegionConfig region = new BasicRegionConfig();
    region.setName("customer");

    ClusterManagementResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).containsOnlyKeys("server-1", "server-2");

    result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.ENTITY_EXISTS);
  }

  @Test
  public void createRegionWithNullGroup() {
    BasicRegionConfig region = new BasicRegionConfig();
    region.setName("orders");

    ClusterManagementResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).containsOnlyKeys("server-1", "server-2");
  }


  @Test
  public void createRegionWithInvalidName() throws Exception {
    BasicRegionConfig region = new BasicRegionConfig();
    region.setName("__test");

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT);
  }

  @Test
  public void createRegionWithGroup() {
    BasicRegionConfig region = new BasicRegionConfig();
    region.setName("company");
    region.setGroup(groupA);

    ClusterManagementResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    // server 1 should not be in the set
    assertThat(result.getMemberStatuses()).containsOnlyKeys("server-2");

    locator.invoke(() -> {
      InternalConfigurationPersistenceService persistenceService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig clusterCacheConfig = persistenceService.getCacheConfig("cluster", true);
      CacheConfig groupACacheConfig = persistenceService.getCacheConfig("group-a");
      assertThat(CacheElement.findElement(clusterCacheConfig.getRegions(), "company")).isNull();
      assertThat(CacheElement.findElement(groupACacheConfig.getRegions(), "company")).isNotNull();
    });
  }

  @Test
  public void invokeFromClientCacheWithLocatorPool() throws Exception {
    int locatorPort = locator.getPort();
    client = cluster.startClientVM(3, c -> c.withLocatorConnection(locatorPort));

    client.invoke(() -> {
      ClusterManagementService service = ClusterManagementServiceProvider.getService();
      assertThat(service.isConnected()).isTrue();
    });
    client.stop();
  }

  @Test
  public void invokeFromClientCacheWithServerPool() throws Exception {
    int serverPort = server.getPort();
    client = cluster.startClientVM(3, c -> c.withServerConnection(serverPort));

    client.invoke(() -> {
      assertThatThrownBy(() -> ClusterManagementServiceProvider.getService())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("the client needs to have a client pool connected with a locator");
    });
    client.stop();
  }
}