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
package org.apache.geode.cache.query.dunit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState.EVICTION_DISABLED;
import static org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState.EVICTION_DISABLED_CRITICAL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.control.ResourceListener;
import org.apache.geode.internal.cache.control.TestMemoryThresholdListener;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({OQLQueryTest.class})
public class ResourceManagerWithQueryMonitorDUnitTest implements Serializable {

  private static final String TEST_POOL_NAME = "testPool";
  private static final Logger LOGGER = LogService.getLogger();
  private static final int MAX_TEST_QUERY_TIMEOUT = 4000;
  private static final int TEST_QUERY_TIMEOUT = 1000;
  private static final int CRITICAL_HEAP_USED = 950;
  private static final int NORMAL_HEAP_USED = 500;
  private static final boolean AUTO_LOAD_BALANCE = false;

  private static InternalCache cache;
  private static CountDownLatch criticalMemoryCountDownLatch;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public final void setUp() {
    Invoke.invokeInEveryVM(() -> {
      HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
      return null;
    });
    IgnoredException.addIgnoredException("above heap critical threshold");
    IgnoredException.addIgnoredException("below heap critical threshold");
    criticalMemoryCountDownLatch = new CountDownLatch(1);
  }

  @After
  public final void tearDown() {
    Invoke.invokeInEveryVM(() -> {
      if (cache.getQueryMonitor() != null) {
        cache.getQueryMonitor().setLowMemory(false, 0);
      }
      DefaultQuery.testHook = null;
      return null;
    });

    Invoke.invokeInEveryVM(() -> {
      InternalResourceManager internalResourceManager = cache.getInternalResourceManager();
      // Reset CRITICAL_UP by informing all that heap usage is now 1 byte (0 would disable).
      internalResourceManager.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED, "test");
      Set<ResourceListener> listeners =
          internalResourceManager.getResourceListeners(ResourceType.HEAP_MEMORY);
      for (ResourceListener listener : listeners) {
        if (listener instanceof TestMemoryThresholdListener) {
          ((TestMemoryThresholdListener) listener).resetThresholdCalls();
        }
      }
      internalResourceManager.setCriticalHeapPercentage(0f);
      internalResourceManager.setEvictionHeapPercentage(0f);
      internalResourceManager.getHeapMonitor().setTestMaxMemoryBytes(0);
      HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
      return null;
    });
    disconnectAllFromDS();
  }

  @Test
  public void testRMAndNoTimeoutSet() {
    doCriticalMemoryHitTest(false, false, -1, true);
  }

  @Test
  public void testRMAndNoTimeoutSetParReg() {
    doCriticalMemoryHitTest(true, false, -1, true);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSet() {
    // verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTest(false, true, -1, true);
  }

  @Test
  public void testRMAndTimeoutSet() {
    // verify that we still receive critical heap cancellation
    doCriticalMemoryHitTest(false, true, TEST_QUERY_TIMEOUT,
        true);
  }

  @Test
  public void testRMAndTimeoutSetAndQueryTimeoutInstead() {
    // verify that timeout is set correctly and cancel query
    doCriticalMemoryHitTest(false, true, TEST_QUERY_TIMEOUT,
        false);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSet() {
    // verify that timeout is still working properly
    doCriticalMemoryHitTest(false, true, TEST_QUERY_TIMEOUT,
        true);
  }

  // Query directly on member with RM and QM set
  @Test
  public void testRMAndNoTimeoutSetOnServer() {
    doCriticalMemoryHitTestOnServer(false, false, -1, true);
  }

  // Query directly on member with RM and QM set
  @Test
  public void whenTimeoutIsSetAndAQueryIsExecutedThenTimeoutMustStopTheQueryBeforeCriticalMemory() {
    // Timeout is set along with critical heap but it be called after the timeout expires
    // Timeout is set to 1ms which is very unrealistic time period for a query to be able to fetch
    // 200 entries from the region successfully, hence a timeout is expected.
    // create region on the server
    final VM server = VM.getVM(0);
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.invoke(() -> startCacheServer(port, false, 1, false));
      // startCacheServer(server, port, false, 1, false);
      populateData(server);
      executeQueryWithCriticalHeapCalledAfterTimeout(server, server);
      vmRecoversFromCriticalHeap(server);

    } finally {
      stopServer(server);
    }
  }

  @Test
  public void whenTimeoutIsSetAndAQueryIsExecutedFromClientThenTimeoutMustStopTheQueryBeforeCriticalMemory() {
    // Timeout is set along with critical heap but it be called after the timeout expires
    // Timeout is set to 1ms which is very unrealistic time period for a query to be able to fetch
    // 200 entries from the region successfully, hence a timeout is expected.
    // create region on the server
    final VM server = VM.getVM(0);
    final VM client = VM.getVM(1);
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.invoke(() -> startCacheServer(port, false, 1, false));
      // startCacheServer(server, port, false, 1, false);

      client.invoke(() -> startClient(port));
      // startClient(client, port);
      populateData(server);
      executeQueryWithCriticalHeapCalledAfterTimeout(server, client);
      vmRecoversFromCriticalHeap(server);

    } finally {
      stopServer(server);
    }
  }

  @Test
  public void testRMAndNoTimeoutSetParRegOnServer() {
    doCriticalMemoryHitTestOnServer(true, false, -1, true);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSetOnServer() {
    // verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTestOnServer(false, true, -1, true);
  }

  @Test
  public void testRMAndTimeoutSetOnServer() {
    // verify that we still receive critical heap cancellation
    doCriticalMemoryHitTestOnServer(false, true,
        TEST_QUERY_TIMEOUT, true);
  }

  @Test
  public void testRMAndTimeoutSetAndQueryTimeoutInsteadOnServer() {
    // verify that timeout is set correctly and cancel query
    doCriticalMemoryHitTestOnServer(false, true,
        TEST_QUERY_TIMEOUT, false);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSetOnServer() {
    // verify that timeout is still working properly
    doCriticalMemoryHitTestOnServer(false, true,
        TEST_QUERY_TIMEOUT, true);
  }

  @Test
  public void testPRGatherCancellation() throws InterruptedException, Throwable {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;
    final VM controller = VM.getController();

    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      server1.invoke(() -> startCacheServer(port[0], false, -1, true));
      server2.invoke(() -> startCacheServer(port[1], false, -1, true));
      // startCacheServer(server1, port[0], false,-1, true);
      // startCacheServer(server2, port[1], true, -1, true);

      client.invoke(() -> startClient(port[0]));
      // startClient(client, port[0]);
      populateData(server2);

      server1.invoke("create latch test Hook", () -> {
        DefaultQuery.testHook = getPauseHook(true, controller);
      });

      // remove from here to ....
      AsyncInvocation queryExecution1 = executeQueryOnClient(client);

      // Force the query to timeout
      Thread.sleep(2000);

      // We simulate a low memory/critical heap percentage hit
      setHeapToCriticalAndReleaseLatch(server1);

      assertThat(queryExecution1.get(60, SECONDS)).isEqualTo(0);

      verifyDroppedObjectsAndSetHeapToNormal(server1);

      // to here....

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke("Executing query when system is 'Normal'", () -> {
        try {
          Query query = cache.getQueryService().newQuery("Select * From /" + "portfolios");
          SelectResults results = (SelectResults) query.execute();
          assertThat(results.size()).isEqualTo(numObjects);
        } catch (QueryInvocationTargetException | FunctionDomainException | TypeMismatchException
            | NameResolutionException e) {
          fail("");
        }
      });

      // We simulate a low memory/critical heap percentage hit
      setHeapToCriticalAndReleaseLatch(server1);

      AsyncInvocation queryExecution = executeQueryOnClient(client);

      assertThat(queryExecution.get(60, SECONDS)).isEqualTo(0);

      verifyDroppedObjectsAndSetHeapToNormal(server1);

    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  @Test
  public void testPRGatherCancellationWhileGatheringResults() {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      server1.invoke(() -> startCacheServer(port[0], false, -1, true));
      server2.invoke(() -> startCacheServer(port[1], true, -1, true));
      // startCacheServer(server1, port[0], false, -1, true);
      // startCacheServer(server2, port[1], true, -1, true);

      client.invoke(() -> startClient(port[0]));
      // startClient(client, port[0]);
      populateData(server2);

      createCancelDuringGatherTestHook(server1, true, VM.getController());
      client.invoke("executing query to be canceled by gather", () -> {
        QueryService qs;
        try {
          qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          query.execute();
        } catch (ServerOperationException soe) {
          if (soe.getRootCause() instanceof QueryException) {
            QueryException e = (QueryException) soe.getRootCause();
            if (!isExceptionDueToLowMemory(e)) {
              throw new CacheException(soe) {};
            } else {
              return 0;
            }
          }
        } catch (Exception e) {
          throw new CacheException(e) {};
        }
        // assertTrue(((CancelDuringGatherHook)DefaultQuery.testHook).triggeredOOME);
        throw new CacheException("should have hit low memory") {};
      });

      verifyRejectedObjects(server1);

      // Recover from critical heap
      vmRecoversFromCriticalHeap(server1);

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke("Executing query when system is 'Normal'", () -> {
        try {
          QueryService qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          SelectResults results = (SelectResults) query.execute();
          assertThat(results.size()).isEqualTo(numObjects);
        } catch (QueryInvocationTargetException | FunctionDomainException | TypeMismatchException
            | NameResolutionException e) {
          fail("");
        }
      });

      // Recover from critical heap
      vmRecoversFromCriticalHeap(server1);
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  @Test
  public void testPRGatherCancellationWhileAddingResults() {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      server1.invoke(() -> startCacheServer(port[0], false, -1, true));
      server2.invoke(() -> startCacheServer(port[1], true, -1, true));
      // startCacheServer(server1, port[0], false, -1, true);
      // startCacheServer(server2, port[1], true, -1, true);

      client.invoke(() -> startClient(port[0]));
      // startClient(client, port[0]);
      populateData(server2);

      createCancelDuringAddResultsTestHook(server1);
      client.invoke("executing query to be canceled during add results", () -> {
        QueryService qs;
        try {
          qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          query.execute();
          throw new CacheException("should have hit low memory") {};
        } catch (Exception e) {
          handleException(e, true, false, -1);
        }
        return 0;
      });

      verifyRejectedObjects(server1);

      // Recover from critical heap
      vmRecoversFromCriticalHeap(server1);

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke("Executing query when system is 'Normal'", () -> {
        try {
          QueryService qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          SelectResults results = (SelectResults) query.execute();
          assertThat(results.size()).isEqualTo(numObjects);
        } catch (QueryInvocationTargetException | FunctionDomainException | TypeMismatchException
            | NameResolutionException e) {
          fail("");
        }
      });

      // Recover from critical heap
      vmRecoversFromCriticalHeap(server1);
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  @Test
  public void testIndexCreationCancellationPR() {
    doCriticalMemoryHitWithIndexTest(true, false,
        "compact");
  }

  @Test
  public void testIndexCreationCancellation() {
    doCriticalMemoryHitWithIndexTest(false, false,
        "compact");
  }

  @Test
  public void testIndexCreationNoCancellationPR() {
    doCriticalMemoryHitWithIndexTest(true, true,
        "compact");
  }

  @Test
  public void testHashIndexCreationCancellationPR() {
    doCriticalMemoryHitWithIndexTest(true, false,
        "hash");
  }

  @Test
  public void testHashIndexCreationCancellation() {
    // need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest(false, false,
        "hash");
  }

  @Test
  public void testHashIndexCreationNoCancellationPR() {
    // need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest(true, true,
        "hash");
  }

  private AsyncInvocation executeQueryOnClient(VM client) {
    return client.invokeAsync("execute query from client", () -> {
      try {
        Query query1 = cache.getQueryService().newQuery("Select * From /" + "portfolios");
        query1.execute();

        throw new CacheException("Exception should have been thrown due to low memory") {};
      } catch (Exception e2) {
        handleException(e2, true, false, -1);
      }
      return 0;
    });
  }

  private void setHeapToCriticalAndReleaseLatch(VM server1) {
    server1.invoke("vm hits critical heap and counts down latch.", () -> {
      InternalResourceManager resourceManager =
          (InternalResourceManager) cache.getResourceManager();
      resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED, "test");

      await()
          .until(() -> resourceManager.getHeapMonitor().getState() == EVICTION_DISABLED_CRITICAL);

      Thread.sleep(MAX_TEST_QUERY_TIMEOUT);

      // release the hook to have the query throw either a low memory or query timeout
      // unless otherwise configured
      PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
      hook.countDown();
    });
  }

  private void verifyDroppedObjectsAndSetHeapToNormal(VM server1) {
    server1.invoke("verify dropped objects", () -> {
      if (DefaultQuery.testHook instanceof RejectedObjectsInterface) {
        RejectedObjectsInterface rejectedObjectsInterface =
            (RejectedObjectsInterface) DefaultQuery.testHook;
        await()
            .untilAsserted(() -> assertThat(rejectedObjectsInterface.rejectedObjects).isTrue());
      }

      InternalResourceManager resourceManager =
          (InternalResourceManager) cache.getResourceManager();
      resourceManager.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED, "test");

      await().until(() -> resourceManager.getHeapMonitor().getState() == EVICTION_DISABLED);
    });
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int port1,
      int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout,
      int lifetimeTimeout) {
    int[] ports;
    if (port2 != -1) {
      ports = new int[] {port1, port2};
    } else {
      ports = new int[] {port1};
    }
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, pingInterval, idleTimeout, lifetimeTimeout);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int port1,
      int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, port1, port2, establish, redundancy,
        connectionsPerServer, serverGroup, -1, -1,
        -2);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, -1/* pingInterval */, -1/* idleTimeout */,
        -2/* lifetimeTimeout */);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout,
      int lifetimeTimeout) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, null, PoolManager.createFactory(), pingInterval,
        idleTimeout, lifetimeTimeout, -1);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout, int lifetimeTimeout,
      int statisticInterval) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, null, PoolManager.createFactory(), pingInterval,
        idleTimeout, lifetimeTimeout, statisticInterval);
  }

  /**
   * this method creates a client connection pool and configures it. If the ports array is not empty
   * it is used to configure the client pool. Otherwise the pool is configured to use the dunit
   * locator.
   */
  public static Pool configureConnectionPoolWithNameAndFactory(AttributesFactory factory,
      String host, int[] ports,
      boolean establish, int redundancy,
      int connectionsPerServer,
      String serverGroup, String poolName,
      PoolFactory pf, int pingInterval,
      int idleTimeout,
      int lifetimeTimeout,
      int statisticInterval) {

    if (AUTO_LOAD_BALANCE || ports.length == 0) {
      pf.addLocator(host, DistributedTestUtils.getLocatorPort());
    } else {
      for (int port : ports) {
        pf.addServer(host, port);
      }
    }

    // TODO - probably should pass in minConnections rather than connections per server
    if (connectionsPerServer != -1 && ports != null) {
      pf.setMinConnections(connectionsPerServer * ports.length);
    }
    if (pingInterval != -1) {
      pf.setPingInterval(pingInterval);
    }
    if (idleTimeout != -1) {
      pf.setIdleTimeout(idleTimeout);
    }
    if (statisticInterval != -1) {
      pf.setStatisticInterval(statisticInterval);
    }
    if (lifetimeTimeout != -2) {
      pf.setLoadConditioningInterval(lifetimeTimeout);
    }
    if (establish) {
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(redundancy);
      pf.setSubscriptionAckInterval(1);
    }
    if (serverGroup != null) {
      pf.setServerGroup(serverGroup);
    }
    String rpoolName = TEST_POOL_NAME;
    if (poolName != null) {
      rpoolName = poolName;
    }
    Pool pool = pf.create(rpoolName);
    if (factory != null) {
      factory.setPoolName(rpoolName);
    }
    return pool;
  }

  public static <K, V> Pool configureConnectionPool(RegionFactory<K, V> factory, String host,
      int[] ports, boolean establish, int redundancy,
      int connectionsPerServer,
      String serverGroup) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, TEST_POOL_NAME, PoolManager.createFactory(), -1, -1, -2,
        -1);
  }

  public static <K, V> Pool configureConnectionPoolWithNameAndFactory(RegionFactory<K, V> factory,
      String host, int[] ports,
      boolean establish,
      int redundancy,
      int connectionsPerServer,
      String serverGroup,
      String poolName,
      PoolFactory pf) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, poolName, pf, -1, -1, -2, -1);
  }

  public static <K, V> Pool configureConnectionPoolWithNameAndFactory(RegionFactory<K, V> factory,
      String host, int[] ports,
      boolean establish,
      int redundancy,
      int connectionsPerServer,
      String serverGroup,
      String poolName,
      PoolFactory pf,
      int pingInterval,
      int idleTimeout,
      int lifetimeTimeout,
      int statisticInterval) {

    if (AUTO_LOAD_BALANCE || ports.length == 0) {
      pf.addLocator(host, DistributedTestUtils.getLocatorPort());
    } else {
      for (int port : ports) {
        pf.addServer(host, port);
      }
    }

    // TODO - probably should pass in minConnections rather than connections per server
    if (connectionsPerServer != -1 && ports != null) {
      pf.setMinConnections(connectionsPerServer * ports.length);
    }
    if (pingInterval != -1) {
      pf.setPingInterval(pingInterval);
    }
    if (idleTimeout != -1) {
      pf.setIdleTimeout(idleTimeout);
    }
    if (statisticInterval != -1) {
      pf.setStatisticInterval(statisticInterval);
    }
    if (lifetimeTimeout != -2) {
      pf.setLoadConditioningInterval(lifetimeTimeout);
    }
    if (establish) {
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(redundancy);
      pf.setSubscriptionAckInterval(1);
    }
    if (serverGroup != null) {
      pf.setServerGroup(serverGroup);
    }
    String rpoolName = TEST_POOL_NAME;
    if (poolName != null) {
      rpoolName = poolName;
    }
    Pool pool = pf.create(rpoolName);
    if (factory != null) {
      factory.setPoolName(rpoolName);
    }
    return pool;
  }

  protected DistributedMember getMemberId() {
    InternalDistributedSystem internalDistributedSystem = cacheRule.getSystem();
    await("Waiting for client to connect " + internalDistributedSystem.getMemberId())
        .until(() -> internalDistributedSystem.getDistributedMember().getMembershipPort() > 0);
    return internalDistributedSystem.getDistributedMember();
  }

  private void doCriticalMemoryHitTest(boolean createPR,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout, final boolean hitCriticalThreshold) {
    // create region on the server
    final VM server = VM.getVM(0);
    final VM client = VM.getVM(1);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.invoke(
          () -> startCacheServer(port, disabledQueryMonitorForLowMem, queryTimeout, createPR));
      // startCacheServer(server, port, disabledQueryMonitorForLowMem, queryTimeout, createPR);

      client.invoke(() -> startClient(port));
      // startClient(client, port);
      populateData(server);

      doTestCriticalHeapAndQueryTimeout(server, client, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
        await().until(() -> vmCheckCritcalHeap(server) == EVICTION_DISABLED);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke("Executing query when system is 'Normal'", () -> {
        try {
          QueryService qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          SelectResults results = (SelectResults) query.execute();
          assertThat(results.size()).isEqualTo(numObjects);
        } catch (QueryInvocationTargetException | FunctionDomainException | TypeMismatchException
            | NameResolutionException e) {
          fail("");
        }
      });

      // Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, client, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);
      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
        await().until(() -> vmCheckCritcalHeap(server) == EVICTION_DISABLED);
      }
    } finally {
      stopServer(server);
    }
  }

  // test to verify what happens during index creation if memory threshold is hit
  private void doCriticalMemoryHitWithIndexTest(boolean createPR,
      final boolean disabledQueryMonitorForLowMem,
      final String indexType) {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(2);
    final VM client = VM.getVM(1);

    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      server1.invoke(() -> startCacheServer(port[0], disabledQueryMonitorForLowMem, -1, createPR));
      server2.invoke(() -> startCacheServer(port[1], true, -1, createPR));
      // startCacheServer(server1, port[0], disabledQueryMonitorForLowMem, -1, createPR);
      // startCacheServer(server2, port[1], true, -1, createPR);

      client.invoke(() -> startClient(port[0]));
      // startClient(client, port[0]);
      populateData(server1);

      createCancelDuringGatherTestHook(server1, true, VM.getController());
      server1.invoke("create index", () -> {
        QueryService qs;
        try {
          qs = cache.getQueryService();
          Index index = null;
          if (indexType.equals("compact")) {
            index = qs.createIndex("newIndex", "ID", "/" + "portfolios");
          } else if (indexType.equals("hash")) {
            index = qs.createIndex("newIndex", "ID", "/" + "portfolios");
          }
          assertThat(index).isNotNull();
          assertThat(((CancelDuringGatherHook) DefaultQuery.testHook).triggeredOOME).isTrue();

          if (!disabledQueryMonitorForLowMem) {
            throw new CacheException("Should have hit low memory") {};
          }
          assertThat(qs.getIndexes().size()).isEqualTo(1);
        } catch (Exception e) {
          if (e instanceof IndexInvalidException) {
            if (disabledQueryMonitorForLowMem) {
              throw new CacheException("Should not have run into low memory exception") {};
            }
          } else {
            throw new CacheException(e) {};
          }
        }
        return 0;
      });
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }


  // Executes the query on the server with the RM and QM configured
  private void doCriticalMemoryHitTestOnServer(boolean createPR,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold) {
    // create region on the server
    final VM server = VM.getVM(0);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.invoke(
          () -> startCacheServer(port, disabledQueryMonitorForLowMem, queryTimeout, createPR));
      // startCacheServer(server, port, disabledQueryMonitorForLowMem, queryTimeout, createPR);

      populateData(server);

      doTestCriticalHeapAndQueryTimeout(server, server, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      server.invoke("Executing query when system is 'Normal'", () -> {
        try {
          QueryService qs = cache.getQueryService();
          Query query = qs.newQuery("Select * From /" + "portfolios");
          SelectResults results = (SelectResults) query.execute();
          assertThat(results.size()).isEqualTo(numObjects);
        } catch (QueryInvocationTargetException | FunctionDomainException | TypeMismatchException
            | NameResolutionException e) {
          fail("");
        }
      });

      // Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, server, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
    } finally {
      stopServer(server);
    }
  }

  // This helper method will set up a test hook
  // Execute a query on the server, pause due to the test hook
  // Execute a critical heap event
  // release the test hook
  // Check to see that the query either failed due to critical heap if query monitor is not disabled
  // or it will fail due to time out, due to the sleeps we put in
  // If timeout is disabled/not set, then the query should execute just fine
  // The last part of the test is to execute another query with the system under duress and have it
  // be rejected/cancelled if rm and qm are in use
  private void doTestCriticalHeapAndQueryTimeout(VM server, VM client,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold) {
    createLatchTestHook(server, hitCriticalThreshold, VM.getController());

    AsyncInvocation queryExecution = invokeClientQuery(client,
        disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold, VM.getController());

    try {
      criticalMemoryCountDownLatch.await();
      LOGGER.info("MLH: Finished awaiting");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // We simulate a low memory/critical heap percentage hit
    if (hitCriticalThreshold) {
      vmHitsCriticalHeap(server);
      await().until(() -> vmCheckCritcalHeap(server) == EVICTION_DISABLED_CRITICAL);
    }

    // Pause until query would time out if low memory was ignored
    letTimeoutExpire();

    // release the hook to have the query throw either a low memory or query timeout
    // unless otherwise configured
    releaseHook(server);

    try {
      assertThat(queryExecution.get(60, SECONDS)).isEqualTo(0);
    } catch (Throwable e) {
      e.printStackTrace();
      fail("queryExecution.getResult() threw Exception " + e.toString());
    }
  }

  private void letTimeoutExpire() {
    try {
      Thread.sleep(MAX_TEST_QUERY_TIMEOUT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void executeQueryWithCriticalHeapCalledAfterTimeout(VM server, VM client) {
    createLatchTestHook(server, false, VM.getController());
    AsyncInvocation queryExecution = executeQueryWithTimeout(client);

    // Wait till the timeout expires on the query
    try {
      Thread.sleep(1 + 1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // We simulate a low memory/critical heap percentage hit
    // But by design of this test the query must have been already terminated because of a 1ms
    // timeout
    vmHitsCriticalHeap(server);

    releaseHook(server);

    // Make sure no exceptions were thrown during query testing
    try {
      assertThat(queryExecution.get(60, SECONDS)).isEqualTo(0);
    } catch (Throwable e) {
      e.printStackTrace();
      fail("queryExecution.getResult() threw Exception " + e.toString());
    }
  }

  private AsyncInvocation executeQueryWithTimeout(VM client) {
    return client.invokeAsync("execute query from client", () -> {
      QueryService qs;
      try {
        qs = cache.getQueryService();
        Query query = qs.newQuery("Select * From /" + "portfolios");
        query.execute();

      } catch (Exception e) {
        e.printStackTrace();
        if (e instanceof QueryExecutionTimeoutException) {
          LOGGER.info("Query Execution must be terminated by a timeout.");
          return 0;
        }
        if (e instanceof ServerOperationException) {
          ServerOperationException soe = (ServerOperationException) e;
          if (soe.getRootCause() instanceof QueryException) {
            QueryException qe = (QueryException) soe.getRootCause();
            if (isExceptionDueToTimeout(qe)) {
              LOGGER.info("Query Execution must be terminated by a timeout. Expected behavior");
              return 0;
            }
          } else if (soe.getRootCause() instanceof QueryExecutionTimeoutException) {
            LOGGER.info("Query Execution must be terminated by a timeout.");
            return 0;
          }
        }
        e.printStackTrace();
        throw new CacheException(
            "The query should have been terminated by a timeout exception but instead hit a different exception :"
                + e) {};
      }
      return -1;
    });

  }

  private AsyncInvocation invokeClientQuery(VM client,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold,
      VM callbackToVM) {
    return client.invokeAsync("execute query from client", () -> {
      QueryService qs = null;
      try {
        qs = cache.getQueryService();
        Query query = qs.newQuery("Select * From /" + "portfolios");
        callbackToVM
            .invoke(() -> ResourceManagerWithQueryMonitorDUnitTest.criticalMemoryCountDownLatch
                .countDown());
        LOGGER.info("MLH Counted down latch");
        query.execute();
        if (disabledQueryMonitorForLowMem) {
          if (queryTimeout != -1) {
            // we should have timed out due to the way the test is written
            // the query should have hit the configured timeouts
            throw new CacheException("Should have reached the query timeout") {};
          }
        } else {
          if (hitCriticalThreshold) {
            throw new CacheException("Exception should have been thrown due to low memory") {};
          }
        }
      } catch (Exception e) {
        handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
      }

      try {
        Query query = qs.newQuery("Select * From /" + "portfolios");
        query.execute();
        if (hitCriticalThreshold && !disabledQueryMonitorForLowMem) {
          throw new CacheException("Low memory should still be cancelling queries") {};
        }
      } catch (Exception e) {
        handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
      }
      return 0;
    });

  }

  private void handleException(Exception e, boolean hitCriticalThreshold,
      boolean disabledQueryMonitorForLowMem, long queryTimeout)
      throws CacheException {
    if (e instanceof QueryExecutionLowMemoryException) {
      if (!(hitCriticalThreshold && !disabledQueryMonitorForLowMem)) {
        // meaning the query should not be canceled due to low memory
        throw new CacheException("Query should not have been canceled due to memory") {};
      }
    } else if (e instanceof QueryExecutionTimeoutException) {
      // if we have a queryTimeout set
      if (queryTimeout == -1) {
        // no time out set, this should not be thrown
        throw new CacheException(
            "Query failed due to unexplained reason, should not have been a time out or low memory "
                + DefaultQuery.testHook.getClass().getName() + " " + e) {};
      }
    } else if (e instanceof QueryException) {
      if (isExceptionDueToLowMemory((QueryException) e)) {
        if (!(hitCriticalThreshold && !disabledQueryMonitorForLowMem)) {
          // meaning the query should not be canceled due to low memory
          throw new CacheException("Query should not have been canceled due to memory") {};
        }
      } else if (isExceptionDueToTimeout((QueryException) e)) {

        if (queryTimeout == -1) {
          // no time out set, this should not be thrown
          throw new CacheException(
              "Query failed due to unexplained reason, should not have been a time out or low memory") {};
        }
      } else {
        throw new CacheException(e) {};
      }
    } else if (e instanceof ServerOperationException) {
      ServerOperationException soe = (ServerOperationException) e;
      if (soe.getRootCause() instanceof QueryExecutionLowMemoryException) {
        if (!(hitCriticalThreshold && !disabledQueryMonitorForLowMem)) {
          // meaning the query should not be canceled due to low memory
          throw new CacheException("Query should not have been canceled due to memory") {};
        }
      } else if (soe.getRootCause() instanceof QueryException) {
        QueryException qe = (QueryException) soe.getRootCause();
        if (isExceptionDueToLowMemory(qe)) {
          if (!(hitCriticalThreshold && !disabledQueryMonitorForLowMem)) {
            // meaning the query should not be canceled due to low memory
            throw new CacheException("Query should not have been canceled due to memory") {};
          }
        } else if (isExceptionDueToTimeout(qe)) {
          if (queryTimeout == -1) {
            e.printStackTrace();
            // no time out set, this should not be thrown
            throw new CacheException(
                "Query failed due to unexplained reason, should not have been a time out or low memory") {};
          }
        } else {
          throw new CacheException(soe) {};
        }
      } else if (soe.getRootCause() instanceof QueryExecutionTimeoutException) {
        // if we have a queryTimeout set
        if (queryTimeout == -1) {
          // no time out set, this should not be thrown
          throw new CacheException(
              "Query failed due to unexplained reason, should not have been a time out or low memory "
                  + DefaultQuery.testHook.getClass().getName() + " " + soe.getRootCause()) {};
        }
      } else {
        throw new CacheException(soe) {};
      }
    } else {
      throw new CacheException(e) {};
    }
  }

  private void vmHitsCriticalHeap(VM vm) {
    vm.invoke("vm hits critical heap", () -> {
      InternalResourceManager resourceManager =
          (InternalResourceManager) cache.getResourceManager();
      resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED, "test");
    });
  }


  private void vmRecoversFromCriticalHeap(VM vm) {
    vm.invoke("vm hits critical heap", () -> {
      InternalResourceManager resourceManager =
          (InternalResourceManager) cache.getResourceManager();
      resourceManager.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED, "test");
    });
  }

  private MemoryThresholds.MemoryState vmCheckCritcalHeap(VM vm) {
    return vm.invoke("vm hits critical heap", () -> {
      InternalResourceManager resourceManager =
          (InternalResourceManager) cache.getResourceManager();
      return resourceManager.getHeapMonitor().getState();
    });
  }

  private MemoryThresholds.MemoryState vmCheckCritcalHeap() {
    InternalResourceManager resourceManager =
        (InternalResourceManager) cacheRule.getCache().getResourceManager();
    return resourceManager.getHeapMonitor().getState();
  }

  private void createLatchTestHook(VM vm, boolean hitCriticalThreshold,
      VM vmToCallBack) {
    vm.invoke("create latch test Hook", () -> {
      DefaultQuery.testHook = getPauseHook(hitCriticalThreshold, vmToCallBack);
    });
  }

  private void createCancelDuringGatherTestHook(VM vm, boolean hitCriticalThreshold,
      VM vmToCallback) {
    vm.invoke("create cancel during gather test Hook", () -> {
      DefaultQuery.testHook = getCancelDuringGatherHook(hitCriticalThreshold, vmToCallback);
    });
  }

  private void createCancelDuringAddResultsTestHook(VM vm) {
    vm.invoke("create cancel during gather test Hook", () -> {
      DefaultQuery.testHook = getCancelDuringAddResultsHook();
    });
  }


  private void releaseHook(VM vm) {
    vm.invoke("release latch Hook", () -> {
      PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
      hook.countDown();
    });
  }

  // Verify that PRQueryEvaluator dropped objects if low memory
  private void verifyRejectedObjects(VM vm) {
    vm.invoke("verify dropped objects", () -> {
      if (DefaultQuery.testHook instanceof PauseTestHook) {
        PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
        assertThat(hook.rejectedObjects).isTrue();
      } else if (DefaultQuery.testHook instanceof CancelDuringGatherHook) {
        CancelDuringGatherHook hook = (CancelDuringGatherHook) DefaultQuery.testHook;
        assertThat(hook.rejectedObjects).isTrue();
      } else if (DefaultQuery.testHook instanceof CancelDuringAddResultsHook) {
        CancelDuringAddResultsHook hook = (CancelDuringAddResultsHook) DefaultQuery.testHook;
        assertThat(hook.rejectedObjects).isTrue();
      }
    });
  }

  private void populateData(VM vm) {
    vm.invoke("populate data for " + "portfolios", () -> {
      Region<String, Portfolio> region = cache.getRegion("portfolios");
      for (int i = 0; i < 200; i++) {
        region.put("key_" + i, new Portfolio(i));
      }
    });
  }

  private void stopServer(VM server) {
    server.invoke(() -> {
      GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;
      cacheImpl.MAX_QUERY_EXECUTION_TIME = -1;
      return null;
    });
  }

  private void startCacheServer(final int port,
      final boolean disableQueryMonitorForLowMemory,
      final int queryTimeout,
      final boolean createPR) throws IOException {

    // getSystem(getServerProperties());
    if (disableQueryMonitorForLowMemory) {
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY",
          "true");
    } else {
      System.clearProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");
    }

    cache = cacheRule.getOrCreateCache(getServerProperties());

    GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;

    cacheImpl.MAX_QUERY_EXECUTION_TIME = queryTimeout;

    InternalResourceManager resourceManager =
        (InternalResourceManager) cache.getResourceManager();
    HeapMemoryMonitor heapMonitor = resourceManager.getHeapMonitor();
    heapMonitor.setTestMaxMemoryBytes(1000);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(NORMAL_HEAP_USED);
    resourceManager.setCriticalHeapPercentage(85);

    RegionFactory<String, Portfolio> factory = cache.createRegionFactory();
    if (createPR) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(0);
      paf.setTotalNumBuckets(11);
      factory.setPartitionAttributes(paf.create());
    } else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    factory.create("portfolios");
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(port);
    cacheServer.start();

  }

  private void startClient(final int port) {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(getClientProps());
    clientCacheFactory.addPoolServer(NetworkUtils.getServerHostName(), port);
    clientCacheRule.createClientCache(clientCacheFactory);
  }

//  private InternalClientCache getClientCache() {
//    return clientCacheRule.getClientCache();
//  }

  private Properties getClientProps() {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    return properties;
  }

  private Properties getServerProperties() {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getLocatorPort() + "]");
    return properties;
  }

  private boolean isExceptionDueToLowMemory(QueryException e) {
    String message = e.getMessage();
    return (message.contains(
        String.format(
            "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
            ResourceManagerWithQueryMonitorDUnitTest.CRITICAL_HEAP_USED))
        || message.contains(
            "Query execution canceled due to low memory while gathering results from partitioned regions"));
  }

  private boolean isExceptionDueToTimeout(QueryException e) {
    String message = e.getMessage();
    // -1 needs to be matched due to client/server set up, BaseCommand uses the
    // MAX_QUERY_EXECUTION_TIME and not the testMaxQueryExecutionTime

    LOGGER.info("MLH exception was ", e);
    return (message.contains("The QueryMonitor thread may be sleeping longer than")
        || message.contains("Query execution canceled after exceeding max execution time")
        || message.contains(
            String.format("Query execution canceled after exceeding max execution time %sms.",
                -1)));
  }

  private DefaultQuery.TestHook getPauseHook(boolean hitCriticalThreshold,
      VM vmToCallback) {
    return new PauseTestHook(hitCriticalThreshold, vmToCallback);
  }

  private DefaultQuery.TestHook getCancelDuringGatherHook(boolean hitCriticalThreshold,
      VM vmToCallback) {
    return new CancelDuringGatherHook(hitCriticalThreshold, vmToCallback);
  }

  private DefaultQuery.TestHook getCancelDuringAddResultsHook() {
    return new CancelDuringAddResultsHook();
  }

  /**
   * Starts a cache server on the given port
   *
   * @since GemFire 4.0
   */
  public int startBridgeServer(int port) throws IOException {

    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(getMaxThreads());
    bridge.start();
    return bridge.getPort();
  }

  /**
   * Defaults to 0 which means no selector in server. Subclasses can override setting this to a
   * value > 0 to enable selector.
   */
  protected int getMaxThreads() {
    return 0;
  }

  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    return factory.create();
  }

  class RejectedObjectsInterface {
    boolean rejectedObjects = false;
  }

  private class PauseTestHook extends RejectedObjectsInterface implements DefaultQuery.TestHook {
    private final CountDownLatch latch = new CountDownLatch(1);

    boolean hitCriticalThreshold;
    AtomicBoolean hitOnce = new AtomicBoolean(false);
    VM callbackVM;

    PauseTestHook(boolean hitCriticalThreshold, VM vmToCallback) {
      super();
      this.hitCriticalThreshold = hitCriticalThreshold;
      callbackVM = vmToCallback;
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      switch (spot) {
        case BEFORE_QUERY_EXECUTION:
          try {
            if (!latch.await(8, SECONDS)) {
              fail("query was never unlatched");
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
          break;
        case BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION:
          if (hitCriticalThreshold && hitOnce.compareAndSet(false, true)) {
            InternalResourceManager resourceManager =
                (InternalResourceManager) cache.getResourceManager();
            resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED, "test");
            await().until(() -> vmCheckCritcalHeap() == EVICTION_DISABLED_CRITICAL);

            callbackVM
                .invoke(() -> ResourceManagerWithQueryMonitorDUnitTest.criticalMemoryCountDownLatch
                    .countDown());
          }
          break;
        case LOW_MEMORY_WHEN_DESERIALIZING_STREAMINGOPERATION:
          rejectedObjects = true;
          break;
        default:
          break;
      }
    }

    void countDown() {
      latch.countDown();
    }
  }

  private class CancelDuringGatherHook extends RejectedObjectsInterface
      implements DefaultQuery.TestHook {
    boolean triggeredOOME = false;
    private int count = 0;
    boolean hitCriticalThreshold;
    AtomicBoolean hitOnce = new AtomicBoolean(false);
    VM callbackVM;


    CancelDuringGatherHook(boolean hitCriticalThreshold, VM vmToCallback) {
      super();
      this.hitCriticalThreshold = hitCriticalThreshold;
      callbackVM = vmToCallback;
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      int numObjectsBeforeCancel = 5;
      switch (spot) {
        case LOW_MEMORY_WHEN_DESERIALIZING_STREAMINGOPERATION:
          rejectedObjects = true;
          break;
        case BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION:
          if (count++ == numObjectsBeforeCancel) {
            if (hitCriticalThreshold && hitOnce.compareAndSet(false, true)) {
              InternalResourceManager resourceManager =
                  (InternalResourceManager) cache.getResourceManager();
              resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED, "test");
              await().until(() -> vmCheckCritcalHeap() == EVICTION_DISABLED_CRITICAL);
              callbackVM
                  .invoke(
                      () -> ResourceManagerWithQueryMonitorDUnitTest.criticalMemoryCountDownLatch
                          .countDown());
            }
            triggeredOOME = true;
          }
          break;
      }
    }
  }

  private class CancelDuringAddResultsHook extends RejectedObjectsInterface
      implements DefaultQuery.TestHook {
    boolean triggeredOOME = false;

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      switch (spot) {
        case BEFORE_BUILD_CUMULATIVE_RESULT:
          if (!triggeredOOME) {
            InternalResourceManager resourceManager =
                (InternalResourceManager) cache.getResourceManager();
            resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED, "test");
            triggeredOOME = true;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          break;
        case BEFORE_THROW_QUERY_CANCELED_EXCEPTION:
          rejectedObjects = true;
          break;
      }
    }
  }
}
