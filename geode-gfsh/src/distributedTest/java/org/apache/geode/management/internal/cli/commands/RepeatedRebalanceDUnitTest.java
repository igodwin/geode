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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class RepeatedRebalanceDUnitTest implements Serializable {
  private static final String PARENT_REGION = "RepeatedRebalanceTestRegion";
  private static final String COLOCATED_REGION_ONE = "RepeatedRebalanceColocatedRegionOne";
  private static final String COLOCATED_REGION_TWO = "RepeatedRebalanceColocatedRegionTwo";
  private static final String PARTITION_RESOLVER =
      "org.apache.geode.management.internal.cli.commands.RepeatedRebalancePartitionResolver";
  private static final int TOTAL_NUM_BUCKETS = 48;
  private static final int NUM_REDUNDANT_COPIES = 2;
  private static final int NUMBER_OF_ENTRIES = 30000;
  private static final String LOCATOR_NAME = "locator";
  private static final String SERVER1_NAME = "server1";
  private static final String SERVER2_NAME = "server2";
  private static final String SERVER3_NAME = "server3";
  private static final String SERVER4_NAME = "server4";
  private static final String SERVER5_NAME = "server5";
  private static final String SERVER6_NAME = "server6";
  private static final AtomicReference<LocatorLauncher> LOCATOR_LAUNCHER = new AtomicReference<>();
  private static final AtomicReference<ServerLauncher> SERVER_LAUNCHER = new AtomicReference<>();

  private VM server1;
  private VM server5;
  private VM server6;
  private File server1Dir;

  private String locatorString;
  private int locatorPort;
  private int locatorJmxPort;
  private int locatorHttpPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {

    VM locator = getVM(0);
    server1 = getVM(1);
    VM server2 = getVM(2);
    VM server3 = getVM(3);
    VM server4 = getVM(4);
    server5 = getVM(5);
    server6 = getVM(6);

    File locatorDir = temporaryFolder.newFolder(LOCATOR_NAME);
    server1Dir = temporaryFolder.newFolder(SERVER1_NAME);
    File server2Dir = temporaryFolder.newFolder(SERVER2_NAME);
    File server3Dir = temporaryFolder.newFolder(SERVER3_NAME);
    File server4Dir = temporaryFolder.newFolder(SERVER4_NAME);

    int[] ports = getRandomAvailableTCPPorts(3);
    locatorPort = ports[0];
    locatorJmxPort = ports[1];
    locatorHttpPort = ports[2];

    locator.invoke(() -> startLocator(locatorDir, locatorPort, locatorJmxPort, locatorHttpPort));

    locatorString = "localhost[" + locatorPort + "]";

    server1.invoke(() -> startServer(SERVER1_NAME, server1Dir, locatorString));
    server2.invoke(() -> startServer(SERVER2_NAME, server2Dir, locatorString));
    server3.invoke(() -> startServer(SERVER3_NAME, server3Dir, locatorString));
    server4.invoke(() -> startServer(SERVER4_NAME, server4Dir, locatorString));

    gfsh.connectAndVerify(locatorJmxPort, PortType.jmxManager);

    gfsh.executeAndAssertThat("create region --name=" + PARENT_REGION
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + COLOCATED_REGION_ONE
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER
        + " --colocated-with=" + PARENT_REGION)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + COLOCATED_REGION_TWO
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER
        + " --colocated-with=" + COLOCATED_REGION_ONE)
        .statusIsSuccess();
  }

  @Test
  public void testSecondRebalanceIsNotNecessaryWithAddedMembers() throws IOException {

    server1.invoke(this::addDataToRegion);

    File server5Dir = temporaryFolder.newFolder(SERVER5_NAME);
    File server6Dir = temporaryFolder.newFolder(SERVER6_NAME);

    server5.invoke(() -> startServer(SERVER5_NAME, server5Dir, locatorString));
    server6.invoke(() -> startServer(SERVER6_NAME, server6Dir, locatorString));

    // Because we have 2 redundant copies and begin with 4 servers, redundancy is already satisfied
    // before this rebalance. As such we expect to see no redundant copies created.
    TabularResultModelAssert firstRebalance =
        gfsh.executeAndAssertThat("rebalance").statusIsSuccess().hasTableSection("Table2");
    assertRedundancyNotChanged(firstRebalance);
    assertBucketsMoved(firstRebalance);
    assertPrimariesTransfered(firstRebalance);

    TabularResultModelAssert secondRebalance =
        gfsh.executeAndAssertThat("rebalance").statusIsSuccess().hasTableSection("Table2");
    assertRedundancyNotChanged(secondRebalance);
    assertBucketsNotMoved(secondRebalance);
    assertPrimariesNotTransfered(secondRebalance);
  }

  @Test
  public void testSecondRebalanceIsNotNecessaryWithAddedAndRestartedMembers() throws IOException {

    server1.invoke(this::addDataToRegion);

    File server5Dir = temporaryFolder.newFolder(SERVER5_NAME);
    File server6Dir = temporaryFolder.newFolder(SERVER6_NAME);

    server5.invoke(() -> startServer(SERVER5_NAME, server5Dir, locatorString));
    server6.invoke(() -> startServer(SERVER6_NAME, server6Dir, locatorString));

    server1.invoke(() -> {
      SERVER_LAUNCHER.get().stop();
      startServer(SERVER1_NAME, server1Dir, locatorString);
    });

    TabularResultModelAssert firstRebalance =
        gfsh.executeAndAssertThat("rebalance").statusIsSuccess().hasTableSection("Table2");
    assertRedundancyChanged(firstRebalance);
    assertBucketsMoved(firstRebalance);
    assertPrimariesTransfered(firstRebalance);

    TabularResultModelAssert secondRebalance =
        gfsh.executeAndAssertThat("rebalance").statusIsSuccess().hasTableSection("Table2");
    assertRedundancyNotChanged(secondRebalance);
    assertBucketsNotMoved(secondRebalance);
    assertPrimariesNotTransfered(secondRebalance);
  }

  private void addDataToRegion() {
    Cache cache = SERVER_LAUNCHER.get().getCache();
    Region<String, String> region = cache.getRegion(PARENT_REGION);
    Region<String, String> colocatedRegionOne = cache.getRegion(COLOCATED_REGION_ONE);
    Region<String, String> colocatedRegionTwo = cache.getRegion(COLOCATED_REGION_TWO);

    for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
      region.put("key" + i, "value" + i);
      colocatedRegionOne.put("key" + i, "value" + i);
      colocatedRegionTwo.put("key" + i, "value" + i);
    }
  }

  private void assertRedundancyChanged(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(0).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES).last().isNotEqualTo("0");
    tabularResultModelAssert.hasRow(1).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM).last().isNotEqualTo("0");
    tabularResultModelAssert.hasRow(2).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED).last().isNotEqualTo("0");
  }

  private void assertRedundancyNotChanged(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(0)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES, "0");
    tabularResultModelAssert.hasRow(1)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM, "0");
    tabularResultModelAssert.hasRow(2)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED, "0");
  }

  private void assertBucketsMoved(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(3).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES).last().isNotEqualTo("0");
    tabularResultModelAssert.hasRow(4).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME).last().isNotEqualTo("0");
    tabularResultModelAssert.hasRow(5).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED).last()
        .isNotEqualTo("0");
  }

  private void assertBucketsNotMoved(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(3)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES, "0");
    tabularResultModelAssert.hasRow(4)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME, "0");
    tabularResultModelAssert.hasRow(5)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED, "0");
  }

  private void assertPrimariesTransfered(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(6).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME).last().isNotEqualTo("0");
    tabularResultModelAssert.hasRow(7).asList()
        .contains(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED).last()
        .isNotEqualTo("0");
  }

  private void assertPrimariesNotTransfered(TabularResultModelAssert tabularResultModelAssert) {
    tabularResultModelAssert.hasRow(6)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME, "0");
    tabularResultModelAssert.hasRow(7)
        .containsExactly(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED, "0");
  }

  private void startLocator(File directory, int port, int jmxPort, int httpPort) {
    LOCATOR_LAUNCHER.set(new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setPort(port)
        .setWorkingDirectory(directory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, httpPort + "")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, new File(directory, LOCATOR_NAME + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    LOCATOR_LAUNCHER.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR_LAUNCHER.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator" + getVMId())
          .isTrue();
    });
  }

  private void startServer(String name, File workingDirectory, String locator) {
    SERVER_LAUNCHER.set(new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .setDeletePidFileOnStop(true)
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, locator)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    SERVER_LAUNCHER.get().start();
  }
}
