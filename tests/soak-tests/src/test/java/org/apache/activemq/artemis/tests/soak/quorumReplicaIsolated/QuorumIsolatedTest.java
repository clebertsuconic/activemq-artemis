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
package org.apache.activemq.artemis.tests.soak.quorumReplicaIsolated;

import java.io.File;

import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.apache.activemq.artemis.utils.network.NetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/*

To remove network devices:

sudo -n ifconfig lo0 -alias 192.0.2.2
sudo -n ifconfig lo0 -alias 192.0.2.3
sudo -n ifconfig lo0 -alias 192.0.2.4
sudo -n ifconfig lo0 -alias 192.0.2.5
sudo -n ifconfig lo0 -alias 192.0.2.6
sudo -n ifconfig lo0 -alias 192.0.2.7
sudo -n ifconfig lo0 -alias 192.0.2.8
sudo -n ifconfig lo0 -alias 192.0.2.9

To add network devices used on this test:

sudo -n ifconfig lo0 alias 192.0.2.2
sudo -n ifconfig lo0 alias 192.0.2.3
sudo -n ifconfig lo0 alias 192.0.2.4
sudo -n ifconfig lo0 alias 192.0.2.5
sudo -n ifconfig lo0 alias 192.0.2.6
sudo -n ifconfig lo0 alias 192.0.2.7
sudo -n ifconfig lo0 alias 192.0.2.8
sudo -n ifconfig lo0 alias 192.0.2.9
* */
public class QuorumIsolatedTest extends SoakTestBase {


   public static final String LIVE_0 = "QuorumIsolatedFlowControlTest/live0";
   public static final String LIVE_1 = "QuorumIsolatedFlowControlTest/live1";
   public static final String LIVE_2 = "QuorumIsolatedFlowControlTest/live2";

   public static final String BKP_0 = "QuorumIsolatedFlowControlTest/bkp0";
   public static final String BKP_1 = "QuorumIsolatedFlowControlTest/bkp1";
   public static final String BKP_2 = "QuorumIsolatedFlowControlTest/bkp2";


   static String[] hosts = new String[] {"192.0.2.2",
                                         "192.0.2.3",
                                         "192.0.2.4",
                                         "192.0.2.5",
                                         "192.0.2.6",
                                         "192.0.2.7",
                                         "192.0.2.8",
                                         "192.0.2.9"};

   private static String getHost(int i) {
      return hosts[i];
   }


   private static void newServer(String location, int serverID, String replicaGroupName, boolean live, int [] connectedNodes) throws Exception {
      File serverLocation = getFileServerLocation(location);
      deleteDirectory(serverLocation);
      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
      StringBuilder clusterList = new StringBuilder();
      for (int i = 0; i < connectedNodes.length; i++) {
         if (i > 0) {
            clusterList.append(",");
         }
         clusterList.append("tcp://" + getHost(connectedNodes[i]) + ":61616");
      }
      cliCreateServer.setArgs("--host", getHost(serverID), "--clustered", "--static-cluster", clusterList.toString(), "--name", "main0", "--replicated", "--replica-group-name", replicaGroupName);

      if (!live) {
         cliCreateServer.addArgs("--backup");
      }
      cliCreateServer.createServer();
   }

   private static void upNet() throws Exception {
      int deviceID = 0;
      for (String host : hosts) {
         String device = "lo:" + deviceID;
         deviceID++;
         NetUtil.netUp(host, device);
      }
   }

   private static void downNet() throws Exception {
      int deviceID = 0;
      for (String host : hosts) {
         String device = "lo:" + deviceID;
         deviceID++;
         NetUtil.netDown(host, device, true);
      }
   }


   @BeforeAll
   public static void createServers() throws Exception {
      NetUtil.skipIfNotSudo();
      downNet();
      upNet();
      {
         newServer(LIVE_0, 0, "live0", true, new int[] {1, 2, 3, 4, 5});
         newServer(LIVE_1, 1, "live1", true, new int[] {0, 2, 3, 4, 5});
         newServer(LIVE_2, 2, "live2", true, new int[] {0, 1, 3, 4, 5});
         newServer(BKP_0, 3, "live0", false, new int[] {0, 1, 2, 4, 5});
         newServer(BKP_1, 4, "live1", false, new int[] {0, 1, 2, 3, 5});
         newServer(BKP_2, 5, "live2", false, new int[] {0, 1, 2, 3, 4});
      }
   }

   @Test
   public void testCreateServer() throws Exception {
   }
}
