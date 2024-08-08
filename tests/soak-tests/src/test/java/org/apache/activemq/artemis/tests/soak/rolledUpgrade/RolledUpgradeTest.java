/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.soak.rolledUpgrade;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RolledUpgradeTest extends SoakTestBase {


   private static String sourceHome = "/Volumes/SamsungClebert/work/brokers/activemq-artemis-2.28.0.PATCH.3272";
   //private static String sourceHome = HelperBase.getHome().getAbsolutePath();

   public static final String LIVE_0 = "RolledUpgradeTest/live0";
   public static final String LIVE_1 = "RolledUpgradeTest/live1";
   public static final String LIVE_2 = "RolledUpgradeTest/live2";

   public static final String BKP_0 = "RolledUpgradeTest/bkp0";
   public static final String BKP_1 = "RolledUpgradeTest/bkp1";
   public static final String BKP_2 = "RolledUpgradeTest/bkp2";


   Process live0Process;
   Process live1Process;
   Process live2Process;

   Process bkp0Process;
   Process bkp1Process;
   Process bkp2Process;

   private static String getHost(int nodeID) {
      return "localhost";
   }

   private static int getPort(int nodeID) {
      return 61616 + nodeID;
   }

   private static int getPortOffeset(int nodeID) {
      return nodeID;
   }
   public static boolean findReplace(File file, String find, String replace) throws Exception {
      if (!file.exists()) {
         return false;
      }

      String original = Files.readString(file.toPath());
      String newContent = original.replace(find, replace);
      if (!original.equals(newContent)) {
         Files.writeString(file.toPath(), newContent);
         return true;
      } else {
         return false;
      }
   }


   private static void newServer(String homeLocation, String location, int serverID, String replicaGroupName, boolean live, int [] connectedNodes) throws Exception {
      File serverLocation = getFileServerLocation(location);
      List<String> parameters = new ArrayList<>();
      deleteDirectory(serverLocation);

      StringBuilder clusterList = new StringBuilder();
      for (int i = 0; i < connectedNodes.length; i++) {
         if (i > 0) {
            clusterList.append(",");
         }
         clusterList.append("tcp://" + getHost(connectedNodes[i]) + ":" + getPort(connectedNodes[i]));
      }

      parameters.add(homeLocation + "/bin/artemis");
      parameters.add("create");
      parameters.add("--silent");
      parameters.add("--user");
      parameters.add("guest");
      parameters.add("--password");
      parameters.add("guest");
      parameters.add("--port-offset");
      parameters.add(String.valueOf(getPortOffeset(serverID)));
      parameters.add("--allow-anonymous");
      parameters.add("--no-web");
      parameters.add("--no-autotune");
      parameters.add("--host");
      parameters.add("localhost");
      parameters.add("--clustered");
      parameters.add("--staticCluster");
      parameters.add(clusterList.toString());
      parameters.add("--replicated");
      if (!live) {
         parameters.add("--slave");
      }
      parameters.add("--no-amqp-acceptor");
      parameters.add("--no-mqtt-acceptor");
      parameters.add("--no-hornetq-acceptor");
      parameters.add("--no-stomp-acceptor");
      parameters.add(serverLocation.getAbsolutePath());

      ProcessBuilder processBuilder = new ProcessBuilder();
      processBuilder.command(parameters.toArray(new String[parameters.size()]));
      Process process = processBuilder.start();
      SpawnedVMSupport.spawnLoggers(null, null, "ArtemisCreate", true, true, process);
      Assert.assertTrue(process.waitFor(10, TimeUnit.SECONDS));

      File brokerXml = new File(serverLocation, "/etc/broker.xml");

      if (live) {
         Assert.assertTrue(FileUtil.findReplace(brokerXml, "</master>",
     "   <group-name>" + replicaGroupName + "</group-name>\n" +
            "               <check-for-live-server>true</check-for-live-server>\n" +
            "               <quorum-size>2</quorum-size>\n" +
            "            </master>"));
      } else {
         Assert.assertTrue(FileUtil.findReplace(brokerXml, "<slave/>",
                                         "<slave>\n" +
                                                 "              <group-name>" + replicaGroupName + "</group-name>\n" +
                                                 "              <allow-failback>false</allow-failback>\n" +
                                                 "              <quorum-size>2</quorum-size>\n" +
                                                 "            </slave>"));
      }

   }

   @BeforeClass
   public static void createServers() throws Exception {
      newServer(sourceHome, LIVE_0, 0, "live0", true, new int[] {1, 2, 3, 4, 5});
      newServer(sourceHome, LIVE_1, 1, "live1", true, new int[] {0, 2, 3, 4, 5});
      newServer(sourceHome, LIVE_2, 2, "live2", true, new int[] {0, 1, 3, 4, 5});
      newServer(sourceHome, BKP_0, 3, "live0", false, new int[] {0, 1, 2, 4, 5});
      newServer(sourceHome, BKP_1, 4, "live1", false, new int[] {0, 1, 2, 3, 5});
      newServer(sourceHome, BKP_2, 5, "live2", false, new int[] {0, 1, 2, 3, 4});


      /*

      newServerCLI(sourceHome, LIVE_0, 0, "live0", true, new int[] {1, 2, 3, 4, 5});
      newServerCLI(sourceHome, LIVE_1, 1, "live1", true, new int[] {0, 2, 3, 4, 5});
      newServerCLI(sourceHome, LIVE_2, 2, "live2", true, new int[] {0, 1, 3, 4, 5});
      newServerCLI(sourceHome, BKP_0, 3, "live0", false, new int[] {0, 1, 2, 4, 5});
      newServerCLI(sourceHome, BKP_1, 4, "live1", false, new int[] {0, 1, 2, 3, 5});
      newServerCLI(sourceHome, BKP_2, 5, "live2", false, new int[] {0, 1, 2, 3, 4}); */
   }


   private void upgrade(File home, File instance) throws Exception {
      ProcessBuilder upgradeBuilder = new ProcessBuilder();
      upgradeBuilder.command(home.getAbsolutePath() + "/bin/artemis", "upgrade", instance.getAbsolutePath());
      Process process = upgradeBuilder.start();
      SpawnedVMSupport.spawnLoggers(null, null, null, true, true, process);
      Assert.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
   }


   public static File getHome() {
      return getHome("artemis.distribution.output");
   }

   public static File getHome(String homeProperty) {
      String valueHome = System.getProperty(homeProperty);
      if (valueHome == null) {
         throw new IllegalArgumentException("System property " + valueHome + " not defined");
      }
      return new File(valueHome);
   }


   @Test
   public void testJustUpgrade() throws Exception {
      upgrade(getHome(), getFileServerLocation(LIVE_0));
      upgrade(getHome(), getFileServerLocation(LIVE_1));
      upgrade(getHome(), getFileServerLocation(LIVE_2));

      upgrade(getHome(), getFileServerLocation(BKP_0));
      upgrade(getHome(), getFileServerLocation(BKP_1));
      upgrade(getHome(), getFileServerLocation(BKP_2));
   }


   @Test
   public void testUpgradeOnlyLives() throws Exception {
      upgrade(getHome(), getFileServerLocation(LIVE_0));
      upgrade(getHome(), getFileServerLocation(LIVE_1));
      upgrade(getHome(), getFileServerLocation(LIVE_2));

      live0Process = startServer(LIVE_0, 0, 30_000);
      live1Process = startServer(LIVE_1, 1, 30_000);
      live2Process = startServer(LIVE_2, 2, 30_000);

      SimpleManagement managementLive0 = new SimpleManagement("tcp://localhost:61616", null, null);
      SimpleManagement managementLive1 = new SimpleManagement("tcp://localhost:61617", null, null);
      SimpleManagement managementLive2 = new SimpleManagement("tcp://localhost:61618", null, null);

      SimpleManagement managementBKP0 = new SimpleManagement("tcp://localhost:61619", null, null);
      SimpleManagement managementBKP1 = new SimpleManagement("tcp://localhost:61620", null, null);
      SimpleManagement managementBKP2 = new SimpleManagement("tcp://localhost:61621", null, null);


      bkp0Process = startServer(BKP_0, 0, 0);
      bkp1Process = startServer(BKP_1, 0, 0);
      bkp2Process = startServer(BKP_2, 0, 0);

      Wait.assertTrue(managementLive0::isReplicaSync, 10_000, 100);
      Wait.assertTrue(managementLive1::isReplicaSync, 10_000, 100);
      Wait.assertTrue(managementLive2::isReplicaSync, 10_000, 100);

   }


   @Test
   public void testUpgradeServers() throws Exception {
      live0Process = startServer(LIVE_0, 0, 30_000);
      live1Process = startServer(LIVE_1, 1, 30_000);
      live2Process = startServer(LIVE_2, 2, 30_000);

      SimpleManagement managementLive0 = new SimpleManagement("tcp://localhost:61616", null, null);
      SimpleManagement managementLive1 = new SimpleManagement("tcp://localhost:61617", null, null);
      SimpleManagement managementLive2 = new SimpleManagement("tcp://localhost:61618", null, null);

      SimpleManagement managementBKP0 = new SimpleManagement("tcp://localhost:61619", null, null);
      SimpleManagement managementBKP1 = new SimpleManagement("tcp://localhost:61620", null, null);
      SimpleManagement managementBKP2 = new SimpleManagement("tcp://localhost:61621", null, null);


      bkp0Process = startServer(BKP_0, 0, 0);
      bkp1Process = startServer(BKP_1, 0, 0);
      bkp2Process = startServer(BKP_2, 0, 0);

      Wait.assertTrue(managementLive0::isReplicaSync, 10_000, 100);
      Wait.assertTrue(managementLive1::isReplicaSync, 10_000, 100);
      Wait.assertTrue(managementLive2::isReplicaSync, 10_000, 100);

      Pair<Process, Process> updatedProcess;

      updatedProcess = rollUpgrade(0, LIVE_0, managementLive0, live0Process, 3, BKP_0, managementBKP0, bkp0Process);
      this.live0Process = updatedProcess.getA();
      this.bkp0Process = updatedProcess.getB();

      updatedProcess = rollUpgrade(1, LIVE_1, managementLive1, live1Process, 4, BKP_1, managementBKP1, bkp1Process);
      this.live1Process = updatedProcess.getA();
      this.bkp1Process = updatedProcess.getB();

      updatedProcess = rollUpgrade(2, LIVE_2, managementLive2, live2Process, 5, BKP_2, managementBKP2, bkp2Process);
      this.live2Process = updatedProcess.getA();
      this.bkp2Process = updatedProcess.getB();
   }

   protected static void stopServerWithFile(String serverLocation) throws IOException {
      File serverPlace = new File(serverLocation);
      File etcPlace = new File(serverPlace, "etc");
      File stopMe = new File(etcPlace, STOP_FILE_NAME);
      Assert.assertTrue(stopMe.createNewFile());
   }

   protected static void stopServerWithFile(String serverLocation, Process process, int timeout, TimeUnit unit) throws Exception {
      stopServerWithFile(serverLocation);
      process.waitFor(timeout, unit);
   }



   private Pair<Process, Process> rollUpgrade(int liveID, String liveServerToStop, SimpleManagement liveManagement, Process liveProcess,
                                              int backupID, String backupToStop, SimpleManagement backupManagement, Process backupProcess) throws Exception {
      stopServerWithFile(getServerLocation(liveServerToStop), liveProcess, 10, TimeUnit.SECONDS);
      // waiting backup to activate after the live stop
      ServerUtil.waitForServerToStart(backupID, 30_000);

      upgrade(getHome(), getFileServerLocation(liveServerToStop));

      Process newLiveProcess = startServer(liveServerToStop, 0, 0);
      Wait.assertTrue(() -> repeatCallUntilConnected(backupManagement::isReplicaSync), 10_000, 100);

      stopServerWithFile(getServerLocation(backupToStop), backupProcess, 10, TimeUnit.SECONDS);
      // waiting former live to activate after stopping backup
      ServerUtil.waitForServerToStart(liveID, 30_000);

      upgrade(getHome(), getFileServerLocation(BKP_0));

      Process newBackupProcess = startServer(backupToStop, 0, 0);
      Wait.assertTrue(() -> repeatCallUntilConnected(liveManagement::isReplicaSync), 10_000, 100);

      return new Pair<>(newLiveProcess, newBackupProcess);
   }

   boolean repeatCallUntilConnected(Wait.Condition condition) {
      Throwable lastException = null;
      long timeLimit = System.currentTimeMillis() + 10_000;
      do {
         try {
            return condition.isSatisfied();
         } catch (Throwable e) {
            lastException = e;
         }
      } while (System.currentTimeMillis() > timeLimit);

      if (lastException != null) {
         throw new RuntimeException(lastException.getMessage(), lastException);
      } else {
         // if it gets here I'm a bad programmer!
         throw new IllegalStateException("didn't complete for some reason");
      }
   }
}
