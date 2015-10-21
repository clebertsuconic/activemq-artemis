/**
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

package org.apache.activemq.broker.artemiswrapper;

import java.io.File;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class OpenwireArtemisBaseTest {
   @Rule
   public TemporaryFolder temporaryFolder;


   public OpenwireArtemisBaseTest() {
      File tmpRoot = new File("./target/tmp");
      tmpRoot.mkdirs();
      temporaryFolder = new TemporaryFolder(tmpRoot);

   }


   public String getTmp() {
      return getTmpFile().getAbsolutePath();
   }

   public File getTmpFile() {
      return temporaryFolder.getRoot();
   }

   protected String getJournalDir(int serverID, boolean backup) {
      return getTmp() + "/journal_" + serverID + "_" + backup;
   }

   protected String getBindingsDir(int serverID, boolean backup) {
      return getTmp() + "/binding_" + serverID + "_" + backup;
   }

   protected String getPageDir(int serverID, boolean backup) {
      return getTmp() + "/paging_" + serverID + "_" + backup;
   }

   protected String getLargeMessagesDir(int serverID, boolean backup) {
      return getTmp() + "/paging_" + serverID + "_" + backup;
   }

   public String CLUSTER_PASSWORD = "OPENWIRECLUSTER";



   protected Configuration createConfig(final int serverID) throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl().setJMXManagementEnabled(false).
         setSecurityEnabled(false).setJournalMinFiles(2).setJournalFileSize(100 * 1024).setJournalType(JournalType.NIO).
         setJournalDirectory(getJournalDir(serverID, false)).
         setBindingsDirectory(getBindingsDir(serverID, false)).
         setPagingDirectory(getPageDir(serverID, false)).
         setLargeMessagesDirectory(getLargeMessagesDir(serverID, false)).
         setJournalCompactMinFiles(0).
         setJournalCompactPercentage(0).
         setClusterPassword(CLUSTER_PASSWORD);

      configuration.addAddressesSetting("#", new AddressSettings().setAutoCreateJmsQueues(true).setAutoDeleteJmsQueues(true));

      configuration.addAcceptorConfiguration("netty", newURI(serverID));
      configuration.addConnectorConfiguration("netty-connector", newURI(serverID));

      return configuration;
   }


   public void deployClusterConfiguration(Configuration config, int ... targetIDs) throws Exception {
      StringBuffer stringBuffer = new StringBuffer();
      String separator = "";
      for (int x : targetIDs) {
         stringBuffer.append(separator + newURI(x));
         separator = ",";
      }

      String ccURI = "static://(" + stringBuffer.toString() + ")?connectorName=netty-connector;retryInterval=500;messageLoadBalancingType=STRICT;maxHops=1";

      config.addClusterConfiguration("clusterCC", ccURI);
   }

   protected String newURI(int serverID) {
      return "tcp://localhost:" + (61616 + serverID);
   }

}
