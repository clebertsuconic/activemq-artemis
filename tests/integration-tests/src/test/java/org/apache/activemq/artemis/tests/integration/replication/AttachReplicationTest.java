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
package org.apache.activemq.artemis.tests.integration.replication;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedLockManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.JournalUpdateCallback;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.replication.ReplicatedJournal;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameter;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.activemq.artemis.core.config.impl.Validators.GT_ZERO;
import static org.apache.activemq.artemis.core.config.impl.Validators.MINUS_ONE_OR_GE_ZERO;
import static org.apache.activemq.artemis.core.config.impl.Validators.NOT_NULL_OR_EMPTY;
import static org.apache.activemq.artemis.core.config.impl.Validators.NO_CHECK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public final class AttachReplicationTest extends ActiveMQTestBase {

   private static final String LIVE_URI = "tcp://localhost:61616";
   private static final String BACKUP_URI = "tcp://localhost:61716";

   public ActiveMQServer setupLive() throws Exception {

      Configuration configuration = createDefaultConfig(1, true);
      configuration.clearAcceptorConfigurations();
      configuration.addAcceptorConfiguration("live", LIVE_URI);
      configuration.addConnectorConfiguration("toBackup", BACKUP_URI);

      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration();
      ArrayList<String> connectors = new ArrayList<>();
      connectors.add("toBackup");
      clusterConnectionConfiguration.setStaticConnectors(connectors).setMessageLoadBalancingType(MessageLoadBalancingType.OFF).setMaxHops(1);
      configuration.addClusterConfiguration(clusterConnectionConfiguration);

      ReplicationPrimaryPolicyConfiguration primary = ReplicationPrimaryPolicyConfiguration.withDefault();
      primary.setDistributedManagerConfiguration(new DistributedLockManagerConfiguration());

      DistributedLockManagerConfiguration managerConfiguration =
         new DistributedLockManagerConfiguration(FileBasedLockManager.class.getName(),
                                                 Collections.singletonMap("locks-folder", newFolder(temporaryFolder, "manager").toString()));
      primary.setDistributedManagerConfiguration(managerConfiguration);

      configuration.setHAPolicyConfiguration(primary);

      return createServer(true, configuration);
   }

   public ActiveMQServer setupBackup() throws Exception {

      Configuration configuration = createDefaultConfig(2, true);
      configuration.clearAcceptorConfigurations();
      configuration.addAcceptorConfiguration("backup", BACKUP_URI);
      configuration.addConnectorConfiguration("toLive", LIVE_URI);

      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration();
      ArrayList<String> connectors = new ArrayList<>();
      connectors.add("toLive");
      clusterConnectionConfiguration.setStaticConnectors(connectors).setMessageLoadBalancingType(MessageLoadBalancingType.OFF).setMaxHops(1);
      configuration.addClusterConfiguration(clusterConnectionConfiguration);

      ReplicationBackupPolicyConfiguration backup = ReplicationBackupPolicyConfiguration.withDefault();
      backup.setDistributedManagerConfiguration(new DistributedLockManagerConfiguration());

      DistributedLockManagerConfiguration managerConfiguration =
         new DistributedLockManagerConfiguration(FileBasedLockManager.class.getName(),
                                                 Collections.singletonMap("locks-folder", newFolder(temporaryFolder, "manager").toString()));
      backup.setDistributedManagerConfiguration(managerConfiguration);

      configuration.setHAPolicyConfiguration(backup);

      return createServer(true, configuration);
   }


   @Test
   public void testReplica() throws Exception {
      ActiveMQServer server = setupLive();
      server.start();

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", LIVE_URI);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue("TEST"));
         for (int i = 0; i < 1000; i++) {
            producer.send(session.createTextMessage("hello" + i));
         }
         session.commit();
      }

      ActiveMQServer backupServer = setupBackup();
      backupServer.start();

      Thread.sleep(5000);
   }


   private static File newFolder(File root, String subFolder) throws IOException {
      File result = new File(root, subFolder);
      if (result.exists()) {
         return result;
      }
      if (!result.mkdirs()) {
         throw new IOException("Couldn't create folders " + root);
      }
      return result;
   }


}