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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHostName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListenerAdapter;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests cases that are particular for the auto connection source - dynamically discovering servers,
 * locators, handling locator disappearance, etc.
 */
@Category({ClientServerTest.class})
public class AutoConnectionSourceDUnitTest implements Serializable {
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Object BRIDGE_LISTENER = "BRIDGE_LISTENER";
  private static final long MAX_WAIT = 60000;
  private static final String CACHE_KEY = "CACHE";
  private static final String LOCATOR_KEY = "LOCATOR";
  private static final String REGION_NAME = "A_REGION";
  private static final String POOL_NAME = "daPool";
  private static final Object CALLBACK_KEY = "callback";
  /**
   * A map for storing temporary objects in a remote VM so that they can be used between calls.
   * Cleared after each test.
   */
  private static final HashMap<Object, Object> remoteObjects = new HashMap<>();

  private String hostname;
  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setup() throws UnknownHostException {
    hostname = getLocalHostName();

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    addIgnoredException("NoAvailableLocatorsException");
  }

  @After
  public void teardown() {
    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.bounceForcibly();
    }

    disconnectAllFromDS();
  }

  @Test
  public void testDiscoverBridgeServers() {
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(""));

    String locators = hostname + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startCacheServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> createClientCache(null, hostname, locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, KEY, VALUE);

    assertThat(VALUE).isEqualTo(getInVM(vm1, KEY));
  }

  @Test
  public void testNoLocators() {
    vm0.invoke("StartBridgeClient",
        () -> createClientCache(null, hostname,
            AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET)));
    checkLocators(vm0, new InetSocketAddress[] {});

    vm0.invoke(() -> {
      assertThatThrownBy(
          () -> ((Cache) remoteObjects.get(CACHE_KEY)).getRegion(REGION_NAME).put(KEY, VALUE))
              .isInstanceOf(NoAvailableLocatorsException.class);
    });
  }

  @Test
  public void testNoBridgeServer() {
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(""));
    vm1.invoke("StartBridgeClient", () -> createClientCache(null, hostname, locatorPort));

    assertThatThrownBy(() -> putInVM(vm1))
        .hasRootCauseInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void testDynamicallyFindBridgeServer() {
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(""));

    String locators = getLocatorString(hostname, locatorPort);

    vm1.invoke("Start BridgeServer", () -> startCacheServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> createClientCache(null, hostname, locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, KEY, VALUE);

    vm3.invoke("Start BridgeServer", () -> startCacheServer(null, locators));

    stopBridgeMemberVM(vm1);

    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");

    assertThat(getInVM(vm3, "key2")).isEqualTo("value2");
  }

  @Test
  public void testClientDynamicallyFindsNewLocator() {
    int locator0Port = vm0.invoke("Start Locator1 ", () -> startLocator(""));

    vm2.invoke("StartBridgeClient", () -> createClientCache(null, hostname, locator0Port));

    int locator1Port = vm1.invoke("Start Locator2 ",
        () -> startLocator(getLocatorString(hostname, locator0Port)));

    vm3.invoke("Start BridgeServer",
        () -> {
          int[] locatorPorts = new int[] {locator0Port, locator1Port};
          StringBuffer stringBuffer = new StringBuffer();
          for (int i = 0; i < locatorPorts.length; i++) {
            stringBuffer.append(hostname).append("[").append(locatorPorts[i]).append("]");
            if (i < locatorPorts.length - 1) {
              stringBuffer.append(",");
            }
          }

          return startCacheServer(null, stringBuffer.toString());
        });

    putAndWaitForSuccess(vm2, REGION_NAME, KEY, VALUE);
    assertThat(getInVM(vm3, KEY)).isEqualTo(VALUE);

    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostname, locator1Port);
    waitForLocatorDiscovery(vm2, locatorToWaitFor);
  }

  @Test
  public void testClientDynamicallyDropsStoppedLocator() throws Exception {
    int locator0Port = vm0.invoke("Start Locator1 ", () -> startLocator(""));
    int locator1Port = vm1.invoke("Start Locator2 ",
        () -> startLocator(getLocatorString(hostname, locator0Port)));
    assertThat(locator0Port).isGreaterThan(0);
    assertThat(locator1Port).isGreaterThan(0);

    createClientCache(null, hostname, locator0Port);
    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostname, locator1Port);
    MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    assertThat(discovered).isTrue();

    InetSocketAddress[] initialLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostname, locator0Port)};

    InetSocketAddress[] expectedLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostname, locator0Port),
            new InetSocketAddress(hostname, locator1Port)};

    Pool pool = PoolManager.find(POOL_NAME);

    verifyLocatorsMatched(initialLocators, pool.getLocators());

    verifyLocatorsMatched(expectedLocators, pool.getOnlineLocators());

    // stop one of the locators and ensure that the client can find and use a server
    vm0.invoke("Stop Locator", this::stopLocator);

    await().untilAsserted(() -> assertThat(pool.getOnlineLocators().size()).isEqualTo(1));

    int serverPort = vm2.invoke("Start BridgeServer",
        () -> startCacheServer(null, getLocatorString(hostname, locator1Port)));
    assertThat(serverPort).isGreaterThan(0);

    verifyLocatorsMatched(initialLocators, pool.getLocators());

    InetSocketAddress[] postShutdownLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostname, locator1Port)};
    verifyLocatorsMatched(postShutdownLocators, pool.getOnlineLocators());

    await().untilAsserted(
        () -> assertThatCode(
            () -> ((Cache) remoteObjects.get(CACHE_KEY)).getRegion(REGION_NAME).put(KEY, VALUE))
                .doesNotThrowAnyException());
    assertThat(getInVM(vm2, KEY)).isEqualTo(VALUE);

  }

  @Test
  public void testClientCanUseAnEmbeddedLocator() {
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    String locators = getLocatorString(hostname, locatorPort);

    vm0.invoke("Start BridgeServer", () -> startCacheServerWithEmbeddedLocator(null, locators,
        new String[] {REGION_NAME}, CacheServer.DEFAULT_LOAD_PROBE));

    vm1.invoke("StartBridgeClient",
        () -> createClientCache(null, hostname, locatorPort));

    putAndWaitForSuccess(vm1, REGION_NAME, KEY, VALUE);

    assertThat(getInVM(vm1, KEY)).isEqualTo(VALUE);
  }

  @Test
  public void testClientFindsServerGroups() {
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(""));

    String locators = getLocatorString(hostname, locatorPort);

    vm1.invoke("Start BridgeServer", () -> {
      CacheFactory cacheFactory = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, locators);
      Cache cache = cacheFactory.create();
      for (String regionName : new String[] {"A", "B"}) {
        cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
            .create(regionName);
      }
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.setGroups(new String[] {"group1", "group2"});
      server.setLoadProbe(CacheServer.DEFAULT_LOAD_PROBE);
      server.start();

      remoteObjects.put(CACHE_KEY, cache);

      return server.getPort();
    });
    vm2.invoke("Start BridgeServer", () -> {
      CacheFactory cacheFactory = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, locators);
      Cache cache = cacheFactory.create();
      for (String regionName : new String[] {"B", "C"}) {
        cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
            .create(regionName);
      }
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.setGroups(new String[] {"group2", "group3"});
      server.setLoadProbe(CacheServer.DEFAULT_LOAD_PROBE);
      server.start();

      remoteObjects.put(CACHE_KEY, cache);

      return server.getPort();
    });

    vm3.invoke("StartBridgeClient", () -> createClientCache("group1", hostname,
        locatorPort, new String[] {"A", "B", "C"}));
    putAndWaitForSuccess(vm3, "A", KEY, VALUE);
    assertThat(getInVM(vm1, "A", KEY)).isEqualTo(VALUE);

    vm3.invoke(() -> {
      assertThatThrownBy(() -> {
        ((Cache) remoteObjects.get(CACHE_KEY)).getRegion("C").put("key2", "value2");
      }).hasRootCauseInstanceOf(RegionDestroyedException.class);
    });

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> createClientCache("group3", hostname,
        locatorPort, new String[] {"A", "B", "C"}));

    vm3.invoke(() -> {
      assertThatThrownBy(() -> {
        ((Cache) remoteObjects.get(CACHE_KEY)).getRegion("A").put("key3", VALUE);
      }).hasRootCauseInstanceOf(RegionDestroyedException.class);
    });

    putInVM(vm3, "C", "key4", VALUE);
    assertThat(getInVM(vm2, "C", "key4")).isEqualTo(VALUE);

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> createClientCache("group2", hostname,
        locatorPort, new String[] {"A", "B", "C"}));
    putInVM(vm3, "B", "key5", VALUE);
    assertThat(getInVM(vm1, "B", "key5")).isEqualTo(VALUE);
    assertThat(getInVM(vm2, "B", "key5")).isEqualTo(VALUE);

    stopBridgeMemberVM(vm1);
    putInVM(vm3, "B", "key6", VALUE);
    assertThat(getInVM(vm2, "B", "key6")).isEqualTo(VALUE);
    vm1.invoke("Start BridgeServer", () -> {
      CacheFactory cacheFactory = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, locators);
      Cache cache = cacheFactory.create();
      for (String regionName : new String[] {"A", "B"}) {
        cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
            .create(regionName);
      }
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.setGroups(new String[] {"group1", "group2"});
      server.setLoadProbe(CacheServer.DEFAULT_LOAD_PROBE);
      server.start();

      remoteObjects.put(CACHE_KEY, cache);

      return server.getPort();
    });
    stopBridgeMemberVM(vm2);

    putInVM(vm3, "B", "key7", VALUE);
    assertThat(getInVM(vm1, "B", "key7")).isEqualTo(VALUE);
  }

  @Test
  public void testTwoServersInSameVM() {
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(""));

    String locators = getLocatorString(hostname, locatorPort);

    int serverPort1 =
        vm1.invoke("Start Server", () -> startCacheServer(new String[] {"group1"}, locators));
    int serverPort2 =
        vm1.invoke("Start Server", () -> addCacheServer(new String[] {"group2"}));

    vm2.invoke("Start Client", () -> createClientCache("group2", hostname, locatorPort));

    checkEndpoints(vm2, serverPort2);

    stopBridgeMemberVM(vm2);

    vm2.invoke("Start Client", () -> createClientCache("group1", hostname, locatorPort));

    checkEndpoints(vm2, serverPort1);
  }

  @Test
  public void testClientMembershipListener() {
    int locatorPort =
        vm0.invoke("Start Locator", () -> startLocator(""));

    String locators = getLocatorString(hostname, locatorPort);

    // start a cache server with a listener
    addBridgeListener(vm1);
    int serverPort1 =
        vm1.invoke("Start BridgeServer", () -> startCacheServer(null, locators));

    // start a bridge client with a listener
    addBridgeListener(vm3);
    vm3.invoke("StartBridgeClient",
        () -> createClientCache(null, hostname, locatorPort));
    // wait for client to connect
    checkEndpoints(vm3, serverPort1);

    // make sure the client and cache server both noticed each other
    waitForJoin(vm1);
    MyListener serverListener = (MyListener) vm1
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(serverListener.getCrashes()).isEqualTo(0);
    assertThat(serverListener.getDepartures()).isEqualTo(0);
    assertThat(serverListener.getJoins()).isEqualTo(1);
    resetBridgeListener(vm1);

    waitForJoin(vm3);
    MyListener clientListener = (MyListener) vm3
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(clientListener.getCrashes()).isEqualTo(0);
    assertThat(clientListener.getDepartures()).isEqualTo(0);
    assertThat(clientListener.getJoins()).isEqualTo(1);
    resetBridgeListener(vm3);

    checkEndpoints(vm3, serverPort1);

    // start another cache server and make sure it is detected by the client
    int serverPort2 =
        vm2.invoke("Start BridgeServer", () -> startCacheServer(null, locators));

    checkEndpoints(vm3, serverPort1, serverPort2);
    serverListener = (MyListener) vm1
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(serverListener.getCrashes()).isEqualTo(0);
    assertThat(serverListener.getDepartures()).isEqualTo(0);
    assertThat(serverListener.getJoins()).isEqualTo(0);
    resetBridgeListener(vm1);
    waitForJoin(vm3);
    clientListener = (MyListener) vm3
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(clientListener.getCrashes()).isEqualTo(0);
    assertThat(clientListener.getDepartures()).isEqualTo(0);
    assertThat(clientListener.getJoins()).isEqualTo(1);
    resetBridgeListener(vm3);

    // stop the second cache server and make sure it is detected by the client
    stopBridgeMemberVM(vm2);

    checkEndpoints(vm3, serverPort1);
    serverListener = (MyListener) vm1
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(serverListener.getCrashes()).isEqualTo(0);
    assertThat(serverListener.getDepartures()).isEqualTo(0);
    assertThat(serverListener.getJoins()).isEqualTo(0);
    resetBridgeListener(vm1);
    waitForCrash(vm3);
    clientListener = (MyListener) vm3
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(clientListener.getJoins()).isEqualTo(0);
    assertThat(clientListener.getDepartures() + clientListener.getCrashes()).isEqualTo(1);
    resetBridgeListener(vm3);

    // stop the client and make sure the cache server notices
    stopBridgeMemberVM(vm3);
    waitForDeparture(vm1);
    serverListener = (MyListener) vm1
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
    assertThat(serverListener.getCrashes()).isEqualTo(0);
    assertThat(serverListener.getDepartures()).isEqualTo(1);
    assertThat(serverListener.getJoins()).isEqualTo(0);
  }

  private void waitForLocatorDiscovery(VM vm, final InetSocketAddress locatorToWaitFor) {
    vm.invoke(() -> {
      MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

      boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
      assertThat(discovered).isTrue();
    });
  }

  private Object getInVM(VM vm, final Serializable key) {
    return getInVM(vm, REGION_NAME, key);
  }

  private Object getInVM(VM vm, final String regionName, final Serializable key) {
    return vm.invoke("Get in VM", () -> {
      Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
      Region<String, String> region = cache.getRegion(regionName);
      return region.get(key);
    });
  }

  private void putAndWaitForSuccess(VM vm, final String regionName, final Serializable key,
      final Serializable value) {
    await().untilAsserted(
        () -> assertThatCode(() -> putInVM(vm, regionName, key, value)).doesNotThrowAnyException());
  }

  private void putInVM(VM vm) {
    putInVM(vm, REGION_NAME, KEY, VALUE);
  }

  private void putInVM(VM vm, final String regionName, final Serializable key,
      final Serializable value) {
    vm.invoke("Put in VM",
        () -> {
          Cache cache = ((Cache) remoteObjects.get(CACHE_KEY));
          cache.getRegion(regionName)
              .put(key, value);
        });
  }

  /**
   * Assert that there is one endpoint with the given host in port on the client vm.
   *
   * @param vm - the vm the client is running in
   * @param expectedPorts - The server ports we expect the client to be connected to.
   */
  private void checkEndpoints(VM vm, final int... expectedPorts) {
    vm.invoke("Check endpoint", () -> {
      PoolImpl pool = (PoolImpl) PoolManager.find(POOL_NAME);
      HashSet<Integer> expectedEndpointPorts = new HashSet<>();
      for (int expectedPort : expectedPorts) {
        expectedEndpointPorts.add(expectedPort);
      }
      await().untilAsserted(() -> {
        List<ServerLocation> endpoints;
        HashSet<Integer> actualEndpointPorts;
        endpoints = pool.getCurrentServers();
        actualEndpointPorts = new HashSet<>();
        for (ServerLocation sl : endpoints) {
          actualEndpointPorts.add(sl.getPort());
        }
        assertThat(expectedEndpointPorts).isEqualTo(actualEndpointPorts);
      });
    });
  }

  private Boolean verifyLocatorsMatched(InetSocketAddress[] expected,
      List<InetSocketAddress> initialLocators) {

    if (expected.length != initialLocators.size()) {
      return false;
    }

    Arrays.sort(expected, Comparator.comparing(InetSocketAddress::getPort));

    assertThat(initialLocators).containsExactly(expected);

    for (int i = 0; i < initialLocators.size(); i++) {
      InetSocketAddress locatorAddress = initialLocators.get(i);
      InetSocketAddress expectedAddress = expected[i];

      if (!expectedAddress.equals(locatorAddress)) {
        assertThat(locatorAddress).isEqualTo(expectedAddress);
        return false;
      }
    }
    return true;
  }

  private void checkLocators(VM vm, final InetSocketAddress[] expectedInitial,
      final InetSocketAddress... expected) {
    vm.invoke("Check locators", () -> {
      Pool pool = PoolManager.find(POOL_NAME);

      verifyLocatorsMatched(expectedInitial, pool.getLocators());

      verifyLocatorsMatched(expected, pool.getOnlineLocators());
    });
  }

  private void addBridgeListener(VM vm) {
    vm.invoke("Add membership listener", () -> {
      MyListener listener = new MyListener();
      ClientMembership.registerClientMembershipListener(listener);
      remoteObjects.put(BRIDGE_LISTENER, listener);
    });
  }

  private void resetBridgeListener(VM vm) {
    vm.invoke("Reset membership listener", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      listener.reset();
    });
  }

  private void waitForJoin(VM vm) {
    vm.invoke("wait for join", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      await().until(() -> listener.getJoins() > 0);
    });
  }

  private void waitForCrash(VM vm) {
    vm.invoke("wait for crash", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      await().until(() -> listener.getCrashes() > 0);
    });
  }

  private void waitForDeparture(VM vm) {
    vm.invoke("wait for departure", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      await().until(() -> listener.getDepartures() > 0);
    });
  }

  private String getLocatorString(String hostname, int locatorPort) {
    int[] locatorPorts = new int[] {locatorPort};
    StringBuffer stringBuffer = new StringBuffer();
    for (int i = 0; i < locatorPorts.length; i++) {
      stringBuffer.append(hostname).append("[").append(locatorPorts[i]).append("]");
      if (i < locatorPorts.length - 1) {
        stringBuffer.append(",");
      }
    }

    return stringBuffer.toString();
  }

  private int addCacheServer(final String[] groups) throws IOException {
    Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setGroups(groups);
    server.start();
    return server.getPort();
  }

  private int startLocator(final String otherLocators) throws Exception {
    final int httpPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties properties = new Properties();
    properties.put(MCAST_PORT, String.valueOf(0));
    properties.put(LOCATORS, otherLocators);
    properties.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(HTTP_SERVICE_PORT, String.valueOf(httpPort));
    File logFile = new File("");
    InetAddress bindAddress = getLocalHost();
    Locator locator = Locator.startLocatorAndDS(0, logFile, bindAddress, properties);
    remoteObjects.put(LOCATOR_KEY, locator);
    return locator.getPort();
  }

  private int startCacheServer(String[] groups, String locators) throws IOException {
    CacheFactory cacheFactory = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, locators);
    Cache cache = cacheFactory.create();
    for (String regionName : new String[] {REGION_NAME}) {
      cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
          .create(regionName);
    }
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setGroups(groups);
    server.setLoadProbe(CacheServer.DEFAULT_LOAD_PROBE);
    server.start();

    remoteObjects.put(CACHE_KEY, cache);

    return server.getPort();
  }

  private int startCacheServerWithEmbeddedLocator(final String[] groups, final String locators,
      final String[] regions, final ServerLoadProbe probe) throws IOException {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, locators)
        .set(START_LOCATOR, locators).create();
    for (String regionName : regions) {
      cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
          .create(regionName);
    }
    CacheServer server = cache.addCacheServer();
    server.setGroups(groups);
    server.setLoadProbe(probe);
    server.setPort(0);
    server.start();

    remoteObjects.put(CACHE_KEY, cache);

    return server.getPort();
  }

  private void createClientCache(final String group, final String host, final int port) {
    createClientCache(group, host, port, new String[] {REGION_NAME});
  }

  private void createClientCache(final String group, final String host, final int port,
      final String[] regions) {
    PoolFactoryImpl poolFactory1 = new PoolFactoryImpl(null);
    poolFactory1.addLocator(host, port).setServerGroup(group).setPingInterval(200)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1);

    Cache cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "").create();

    PoolFactoryImpl poolFactory2 = (PoolFactoryImpl) PoolManager.createFactory();
    poolFactory2.init(poolFactory1.getPoolAttributes());
    LocatorDiscoveryCallback locatorCallback = new MyLocatorCallback();

    remoteObjects.put(CALLBACK_KEY, locatorCallback);
    poolFactory2.setLocatorDiscoveryCallback(locatorCallback);
    poolFactory2.create(POOL_NAME);

    RegionFactory<Object, Object> regionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL).setPoolName(POOL_NAME);
    for (String region : regions) {
      regionFactory.create(region);
    }

    remoteObjects.put(CACHE_KEY, cache);
  }

  private void stopLocator() {
    Locator locator = (Locator) remoteObjects.remove(LOCATOR_KEY);
    locator.stop();
  }

  private void stopBridgeMemberVM(VM vm) {
    vm.invoke(() -> {
      Cache cache = (Cache) remoteObjects.remove(CACHE_KEY);
      cache.close();
      disconnectFromDS();
    });
  }

  static class MyListener extends ClientMembershipListenerAdapter implements Serializable {
    volatile int crashes = 0;
    volatile int joins = 0;
    volatile int departures = 0;

    @Override
    public synchronized void memberCrashed(ClientMembershipEvent event) {
      crashes++;
      System.out.println("memberCrashed invoked");
      notifyAll();
    }

    synchronized void reset() {
      crashes = 0;
      joins = 0;
      departures = 0;
    }

    @Override
    public synchronized void memberJoined(ClientMembershipEvent event) {
      joins++;
      System.out.println("memberJoined invoked");
      notifyAll();
    }

    @Override
    public synchronized void memberLeft(ClientMembershipEvent event) {
      departures++;
      System.out.println("memberLeft invoked");
      notifyAll();
    }

    synchronized int getCrashes() {
      return crashes;
    }

    synchronized int getJoins() {
      return joins;
    }

    synchronized int getDepartures() {
      return departures;
    }
  }

  static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();
    private final Set removedLocators = new HashSet();

    @Override
    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    @Override
    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }

    private synchronized boolean waitFor(Set set, InetSocketAddress locator, long time)
        throws InterruptedException {
      long remaining = time;
      long endTime = System.currentTimeMillis() + time;
      while (!set.contains(locator) && remaining >= 0) {
        wait(remaining);
        remaining = endTime - System.currentTimeMillis();
      }
      return set.contains(locator);
    }
  }
}
