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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * Class <code>DurableClientTestCase</code> tests durable client functionality.
 *
 * @since GemFire 5.2
 */
@Category({ClientSubscriptionTest.class})
public class DurableClientTestCase implements Serializable {
  // private static final int HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER = 10;
  private static final int VERY_LONG_DURABLE_TIMEOUT_SECONDS =
      (int) Duration.ofMinutes(10).getSeconds();

  private static InternalCache cache;
  private static InternalClientCache clientCache;
  private static PoolImpl pool;

  private String regionName;
  private VM server1;
  private VM server2;
  private VM durableClient;
  private VM publisherClient;
  private int server1Port;
  private String durableClientId;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTestName serializableTestName = new SerializableTestName();

  @Before
  public void setup() {
    server1 = getVM(0);
    server2 = getVM(1);
    durableClient = getVM(2);
    publisherClient = getVM(3);
    regionName = serializableTestName.getMethodName() + "_region";
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(durableClient, publisherClient, server1, server2)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }
  }

  /**
   * Test that starting a durable client is correctly processed by the server.
   */
  @Test
  public void testSimpleDurableClient() {
    startupDurableClientAndServer(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);

    server1.invoke(() -> cacheRule.closeAndNullCache());

    durableClient.invoke(() -> clientCacheRule.getClientCache().close());
  }

  /**
   * Test that starting a durable client is correctly processed by the server. In this test we will
   * set gemfire.SPECIAL_DURABLE property to true and will see durableID appended by poolname or
   * not
   */
  @Test
  public void testSpecialDurableProperty() {
    final Properties properties = new Properties();
    properties.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "SPECIAL_DURABLE", "true");

    server1Port = server1.invoke(() -> createServerCache());
    // CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    durableClientId = serializableTestName.getMethodName() + "_client";
    final String dId = durableClientId + "_gem_" + "CacheServerTestUtil";

    durableClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port, Boolean.TRUE),
        regionName, getClientDistributedSystemProperties(durableClientId,
            DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT),
        Boolean.TRUE, properties));

    durableClient.invoke(() -> {
      await().untilAsserted(() -> assertThat(cacheRule.getCache()).isNotNull());

      // .atMost(1 * HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
      // .pollInterval(100, MILLISECONDS)
      // .until(CacheServerTestUtil::getCache, notNullValue());
    });

    // Send clientReady message
    // Send clientReady message
    durableClient.invoke(() -> clientCacheRule.getClientCache().readyForEvents());

    // Verify durable client on server
    server1.invoke(() -> {
      // Find the proxy
      checkNumberOfClientProxies(1);
      CacheClientProxy proxy = getClientProxy();
      assertThat(proxy).isNotNull();

      // Verify that it is durable and its properties are correct
      assertThat(proxy.isDurable()).isTrue();
      assertThat(durableClientId).isNotEqualTo(proxy.getDurableId());

      /*
       * new durable id will be like this durableClientId _gem_ //separator client pool name
       */

      assertThat(dId).isEqualTo(proxy.getDurableId());
      assertThat(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT)
          .isEqualTo(proxy.getDurableTimeout());
    });

    // Stop the durable client
    disconnectDurableClient(false);

    // Verify the durable client is present on the server for closeCache=false case.
    verifyDurableClientNotPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        durableClientId, server1);

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());

    durableClient.invoke(() -> clientCacheRule.getClientCache().close());
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server.
   */
  @Test
  public void testStartStopStartDurableClient() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    // Stop the durable client
    disconnectDurableClient(true);

    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1);

    // Re-start the durable client
    startupDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1);

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server. This is a test of bug 39630
   */
  @Test
  public void test39630() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify the durable client still exists on the server, and the socket is closed
    server1.invoke(() -> {
      // Find the proxy
      CacheClientProxy proxy = getClientProxy();
      assertThat(proxy).isNotNull();
      assertThat(proxy._socket).isNotNull();

      await()
          .untilAsserted(() -> assertThat(proxy._socket.isClosed()).isTrue());
    });

    // Re-start the durable client (this is necessary so the
    // netDown test will set the appropriate system properties.
    startupDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }

  /**
   * Test that disconnecting a durable client for longer than the timeout period is correctly
   * processed by the server.
   */
  @Test
  public void testStartStopTimeoutDurableClient() {

    final int durableClientTimeout = 5;
    startupDurableClientAndServer(durableClientTimeout);

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify it no longer exists on the server
    server1.invoke(() -> {
      // Find the proxy
      checkNumberOfClientProxies(0);
      CacheClientProxy proxy = getClientProxy();
      assertThat(proxy).isNull();
    });

    startupDurableClient(durableClientTimeout,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }

  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testDurableClientPrimaryUpdate() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            false),
        regionName));

    // Publish some entries
    publishEntries(0, 1);

    // Wait until queue count is 0 on server1VM
    waitUntilQueueContainsRequiredNumberOfEvents(server1, 0);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClient);

    // Stop the durable client
    disconnectDurableClient(true);

    // Make sure the proxy is actually paused, not dispatching
    server1.invoke(DurableClientTestBase::waitForCacheClientProxyPaused);

    // Publish some more entries
    publishEntries(1, 1);

    // Verify the durable client's queue contains the entries
    waitUntilQueueContainsRequiredNumberOfEvents(server1, 1);

    // Re-start the durable client
    startupDurableClient(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(1, 1, -1, durableClient);

    // Stop the publisher client
    publisherClient.invoke(() -> cacheRule.closeAndNullCache());

    // Stop the durable client VM
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }


  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testStartStopStartDurableClientUpdate() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);
    // Have the durable client register interest in all keys
    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            false),
        regionName));

    // Publish some entries
    publishEntries(0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClient);

    server1.invoke("wait for client acknowledgement", () -> {
      CacheClientProxy proxy = getClientProxy();
      await().untilAsserted(
          () -> assertThat(proxy._messageDispatcher._messageQueue.stats.getEventsRemoved())
              .isGreaterThan(0));
    });

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify the durable client still exists on the server
    server1.invoke(DurableClientTestBase::waitForCacheClientProxyPaused);

    // Publish some entries
    publishEntries(1, 1);

    // Verify the durable client's queue contains the entries
    server1.invoke(() -> {
      CacheClientProxy proxy = getClientProxy();
      assertThat(proxy).isNotNull();
      // Verify the queue size
      assertThat(proxy.getQueueSize()).isEqualTo(1);
    });

    // Re-start the durable client
    startupDurableClient(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(1, 1, -1, durableClient);

    // Stop the publisher client
    publisherClient.invoke(() -> cacheRule.closeAndNullCache());

    // Stop the durable client VM
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }

  /**
   * Test whether a durable client reconnects properly to a server that is stopped and restarted.
   */
  @Test
  public void testDurableClientConnectServerStopStart() {
    // Start a server
    // Start server 1
    Integer[] ports = server1.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = serializableTestName.getMethodName() + "_client";
    durableClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    durableClient.invoke(() -> clientCacheRule.getClientCache().readyForEvents());

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1);

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());

    // Re-start the server
    server1.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort));

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1);

    // Start a publisher
    publisherClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort,
            false),
        regionName));

    // Publish some entries
    publishEntries(0, 10);

    // Verify the durable client received the updates
    checkListenerEvents(10, 1, -1, durableClient);

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the publisher client
    publisherClient.invoke(() -> cacheRule.closeAndNullCache());

    // Stop the server
    server1.invoke(() -> cacheRule.closeAndNullCache());
  }


  @Test
  public void testDurableNonHAFailover() {
    durableFailover(0);
    durableFailoverAfterReconnect(0);
  }

  @Test
  public void testDurableHAFailover() {
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    durableFailover(1);
    durableFailoverAfterReconnect(1);
  }

  private static String printMap(Map<String, Object[]> m) {
    Iterator<Map.Entry<String, Object[]>> itr = m.entrySet().iterator();
    StringBuffer sb = new StringBuffer();
    sb.append("size = ").append(m.size()).append(" ");
    while (itr.hasNext()) {
      sb.append("{");
      Map.Entry<String, Object[]> entry = itr.next();
      sb.append(entry.getKey());
      sb.append(", ");
      printMapValue(entry.getValue(), sb);
      sb.append("}");
    }
    return sb.toString();
  }

  private static void printMapValue(Object value, StringBuffer sb) {
    if (value.getClass().isArray()) {

      sb.append("{");
      sb.append(Arrays.toString((Object[]) value));
      sb.append("}");
    } else {
      sb.append(value);
    }
  }

  private static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = i.next();
    }
    return proxy;
  }

  private static String getAllClientProxyState() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    StringBuilder sb = new StringBuilder();
    while (i.hasNext()) {
      sb.append(" [");
      sb.append(i.next().getState());
      sb.append(" ]");
    }
    return sb.toString();
  }

  private static void checkNumberOfClientProxies(final int expected) {
    await()
        .until(() -> {
          return expected == getNumberOfClientProxies();
        });
  }

  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  private static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer =
        (CacheServerImpl) CacheServerTestUtil.getCache().getCacheServers().iterator().next();
    assertThat(bridgeServer).isNotNull();
    return bridgeServer;
  }

  /**
   * Test a durable client with 2 servers where the client fails over from one to another server
   * with a publisher/feeder performing operations and the client verifying updates received.
   * Redundancy level is set to 1 for this test case.
   */
  private void durableFailover(int redundancyLevel) {

    // Start server 1
    server1Port = server1.invoke(() -> createServerCache());

    // Start server 2 using the same mcast port as server 1
    int server2Port = server2.invoke(() -> createServerCache());

    // Stop server 2
    server2.invoke(() -> cacheRule.closeAndNullCache());

    // Start a durable client
    durableClientId = serializableTestName.getMethodName() + "_client";

    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    durableClient.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    durableClient.invoke(() -> CacheServerTestUtil.createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        Boolean.TRUE));

    // Send clientReady message
    durableClient.invoke(() -> clientCacheRule.getClientCache().readyForEvents());

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    // Re-start server2
    server2.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Start normal publisher client
    publisherClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            server2Port, false),
        regionName));

    // Publish some entries
    publishEntries(0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClient);

    // Stop the durable client, which discards the known entry
    disconnectDurableClient(true);

    // Publish updates during client downtime
    publishEntries(1, 1);

    // Re-start the durable client that is kept alive on the server
    startupDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, clientPool, Boolean.TRUE);

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    publishEntries(2, 1);

    // Verify the durable client received the updates before failover
    checkListenerEvents(2, 1, -1, durableClient);

    durableClient.invoke(() -> {
      Region<Object, Object> region = CacheServerTestUtil.getCache().getRegion(regionName);
      assertThat(region).isNotNull();

      assertThat(region.getEntry("0")).isNull();
      assertThat(region.getEntry("2")).isNotNull();
    });

    // Stop server 1
    server1.invoke(() -> cacheRule.closeAndNullCache());

    // Verify durable client failed over if redundancyLevel=0
    if (redundancyLevel == 0) {
      server2.invoke(() -> verifyClientHasConnected());
    }

    publishEntries(3, 1);

    // Verify the durable client received the updates after failover
    checkListenerEvents(3, 1, -1, durableClient);

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the publisher client
    publisherClient.invoke(() -> cacheRule.closeAndNullCache());

    // Stop server 2
    server2.invoke(() -> cacheRule.closeAndNullCache());
  }

  private void waitUntilQueueContainsRequiredNumberOfEvents(final VM vm,
                                                            final int requiredEntryCount) {
    vm.invoke(() -> {
      await().until(() -> {
        CacheClientProxy proxy = getClientProxy();
        if (proxy == null) {
          return false;
        }
        // Verify the queue size
        int sz = proxy.getQueueSize();
        return requiredEntryCount == sz;
      });
    });
  }

  private void durableFailoverAfterReconnect(int redundancyLevel) {
    // Start server 1
    server1Port = server1
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, true));

    int server2Port = server2
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, true));

    // Start a durable client
    durableClientId = serializableTestName.getMethodName() + "_client";

    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    durableClient.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    durableClient.invoke(() -> CacheServerTestUtil.createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        Boolean.TRUE));

    // Send clientReady message
    durableClient.invoke(() -> clientCacheRule.getClientCache().readyForEvents());

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClient.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            server2Port, false),
        regionName));

    // Publish some entries
    publishEntries(0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClient);

    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1);

    // Stop the durable client
    disconnectDurableClient(true);

    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    server1.invoke(() -> cacheRule.closeAndNullCache());

    // Publish updates during client downtime
    publishEntries(1, 1);

    // Re-start the durable client that is kept alive on the server
    startupDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, clientPool, Boolean.TRUE);

    registerInterest(durableClient, regionName, true, InterestResultPolicy.NONE);

    publishEntries(2, 2);

    // Verify the durable client received the updates before failover
    if (redundancyLevel == 1) {
      checkListenerEvents(4, 1, -1, durableClient);
    } else {
      checkListenerEvents(2, 1, -1, durableClient);
    }

    durableClient.invoke(() -> {
      Region<Object, Object> region = CacheServerTestUtil.getCache().getRegion(regionName);
      assertThat(region).isNotNull();

      // Register interest in all keys
      assertThat(region.getEntry("0")).isNull();
    });

    publishEntries(4, 1);

    // Verify the durable client received the updates after failover
    if (redundancyLevel == 1) {
      checkListenerEvents(5, 1, -1, durableClient);
    } else {
      checkListenerEvents(3, 1, -1, durableClient);
    }

    // Stop the durable client
    durableClient.invoke(() -> clientCacheRule.getClientCache().close());

    // Stop the publisher client
    publisherClient.invoke(() -> cacheRule.closeAndNullCache());

    // Stop server 2
    server2.invoke(() -> cacheRule.closeAndNullCache());
  }

  private void verifyClientHasConnected() {
    CacheServer cacheServer = CacheServerTestUtil.getCache().getCacheServers().get(0);
    CacheClientNotifier ccn =
        ((InternalCacheServer) cacheServer).getAcceptor().getCacheClientNotifier();
    await().until(() -> ccn.getClientProxies().size() == 1);
  }

  private void startupDurableClientAndServer(final int durableClientTimeout) {
    server1Port = server1.invoke(() -> createServerCache());

    durableClientId = serializableTestName.getMethodName() + "_client";
    startupDurableClient(durableClientTimeout,
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), Boolean.TRUE);
    verifyDurableClientPresent(durableClientTimeout, durableClientId, server1);
  }

  private void startupDurableClient(int durableClientTimeout) {
    durableClient.invoke(() -> createClientCache(
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),));

//        CacheServerTestUtil.createCacheClient(
//        clientPool,
//        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
//        addControlListener));

    durableClient.invoke(() -> {
      await().untilAsserted(() -> assertThat(clientCacheRule.getClientCache()).isNotNull());
      // await().atMost(1 * HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
      // .pollInterval(100, MILLISECONDS)
      // .until(CacheServerTestUtil::getCache, notNullValue());
    });

    // Send clientReady message
    // Send clientReady message
    durableClient.invoke(() -> clientCacheRule.getClientCache().readyForEvents());
  }

  private void verifyDurableClientPresent(int durableClientTimeout, String durableClientId,
                                          final VM serverVM) {
    verifyDurableClientPresence(durableClientTimeout, durableClientId, serverVM, 1);
  }

  private void verifyDurableClientNotPresent(int durableClientTimeout, String durableClientId,
                                             final VM serverVM) {
    verifyDurableClientPresence(durableClientTimeout, durableClientId, serverVM, 0);
  }

  private void verifyDurableClientPresence(int durableClientTimeout, String durableClientId,
                                           VM serverVM, final int count) {
    serverVM.invoke(() -> {
      checkNumberOfClientProxies(count);

      if (count > 0) {
        CacheClientProxy proxy = getClientProxy();

        assertThat(proxy).isNotNull();
        // checkProxyIsAlive(proxy);

        // Verify that it is durable and its properties are correct
        assertThat(proxy.isDurable()).isTrue();
        assertThat(durableClientId).isEqualTo(proxy.getDurableId());
        assertThat(durableClientTimeout).isEqualTo(proxy.getDurableTimeout());
      }
    });
  }

  public void disconnectDurableClient(boolean keepAlive) {
    printClientProxyState("Before");
    durableClient.invoke("close durable client cache",
        () -> CacheServerTestUtil.closeCache(keepAlive));
    await().untilAsserted(() -> assertThat(cacheRule.getCache()).isNull());
    // await()
    // .until(CacheServerTestUtil::getCache, nullValue());
    printClientProxyState("after");
  }

  private void printClientProxyState(String st) {
    server1.invoke(() -> {
      // TODO Auto-generated method stub
      CacheServerTestUtil.getCache().getLogger()
          .info(st + " CCP states: " + getAllClientProxyState());
      CacheServerTestUtil.getCache().getLogger().info(st + " CHM states: "
          + printMap(
          ClientHealthMonitor.getInstance().getConnectedClients(null)));
    });
  }

  /*
   * Due to the way removal from ha region queue is implemented a dummy cq or interest needs to be
   * created and a dummy value used so that none of the actual cqs will be triggered and yet an
   * event will flush the queue
   */
  private void flushEntries(VM server, VM client, final String regionName) {
    // This wait is to make sure that all acks have been responded to...
    // We can add a stat later on the cache client proxy stats that checks
    // ack counts
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    registerInterest(client, regionName, false, InterestResultPolicy.NONE);
    server.invoke(() -> {
      Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
      assertThat(region).isNotNull();
      // assertNotNull(region);
      region.put("LAST", "ENTRY");
    });
  }

  private CqQuery createCq(String cqName, String cqQuery, boolean durable)
      throws CqException, CqExistsException {
    QueryService qs = CacheServerTestUtil.getCache().getQueryService();
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new CacheServerTestUtil.ControlCqListener()};
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    return qs.newCq(cqName, cqQuery, cqa, durable);

  }

  private Pool getClientPool(String host, int serverPort, boolean establishCallbackConnection) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, serverPort).setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  private Pool getClientPool(String host, int server1Port, int server2Port,
                             boolean establishCallbackConnection) {
    return getClientPool(host, server1Port, server2Port, establishCallbackConnection, 1);
  }

  private Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  private Properties getClientDistributedSystemProperties(String durableClientId,
                                                          int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  private Pool getClientPool(String host, int server1Port, int server2Port,
                             boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port).addServer(host, server2Port)
        .setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionRedundancy(redundancyLevel).setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  private void registerInterest(VM vm, final String regionName, final boolean durable,
                                final InterestResultPolicy interestResultPolicy) {
    vm.invoke(() -> {
      Region<Object, Object> region = CacheServerTestUtil.getCache().getRegion(regionName);
      assertThat(region).isNotNull();

      // Register interest in all keys
      region.registerInterestRegex(".*", interestResultPolicy, durable);
    });

    // This seems to be necessary for the queue to start up. Ideally should be replaced with
    // Awaitility if possible.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
  }

  // Publishes strings
  private void publishEntries(int startingValue, final int count) {
    publisherClient.invoke(() -> {
      Region<String, String> region = CacheServerTestUtil.getCache().getRegion(
          regionName);
      assertThat(region).isNotNull();

      // Publish some entries
      for (int i = startingValue; i < startingValue + count; i++) {
        String keyAndValue = String.valueOf(i);
        region.put(keyAndValue, keyAndValue);
      }

      assertThat(region.get(String.valueOf(startingValue))).isNotNull();
    });
  }

  private void checkListenerEvents(int numberOfEntries, final int sleepMinutes, final int eventType,
                                   final VM vm) {
    vm.invoke(() -> {
      // Get the region
      Region<Object, Object> region = CacheServerTestUtil.getCache().getRegion(regionName);
      assertThat(region).isNotNull();

      // Get the listener and wait for the appropriate number of events
      CacheServerTestUtil.ControlListener controlListener =
          (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];

      controlListener.waitWhileNotEnoughEvents(sleepMinutes * 60 * 1000, numberOfEntries,
          controlListener.getEvents(eventType));
    });
  }

  private void createClientCache(Properties gemfireProperties, Properties systemProperties) {
    clientCacheRule.createClientCache(gemfireProperties);
    clientCache = clientCacheRule.getClientCache();

    System.setProperties(systemProperties);

    pool = (PoolImpl) PoolManager.createFactory()
        .create("DurableClientReconnectDUnitTestPool");

    clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
        .setPoolName(pool.getName())
        .addCacheListener(new CacheServerTestUtil.ControlListener())
        .create(regionName);
  }

  private int createServerCache() throws Exception {
    cache = cacheRule.getOrCreateCache();

    cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
        .create(regionName);

    CacheServer cacheServer = cache.addCacheServer();

    cacheServer.setMaximumTimeBetweenPings(180000);
    cacheServer.setPort(0);
    cacheServer.start();

    return cacheServer.getPort();
  }
}
