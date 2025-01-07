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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ActiveMQExceptionMessage;
import org.apache.activemq.artemis.core.server.cluster.BackupManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.BackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationPrimaryPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreBackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStorePrimaryPolicy;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.files.FileMoveManager;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class FailoverTest extends FailoverTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected static final int NUM_MESSAGES = 100;

   protected ServerLocator locator;
   protected ClientSessionFactoryInternal sf;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      locator = getServerLocator();
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1) throws Exception {
      return addClientSession(sf1.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   protected void createClientSessionFactory() throws Exception {
      sf = (ClientSessionFactoryInternal) createSessionFactory(locator);
   }

   @Test
   @Timeout(120)
   public void testNonTransacted() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(QueueConfiguration.of(FailoverTestBase.ADDRESS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      session.close();

      sf.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   protected void waitForBackupConfig(ClientSessionFactoryInternal sf) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
      TransportConfiguration initialBackup = getFieldFromSF(sf, "backupConnectorConfig");
      int cnt = 50;
      while (initialBackup == null && cnt > 0) {
         cnt--;
         Thread.sleep(200);
         initialBackup = getFieldFromSF(sf, "backupConnectorConfig");
      }
   }

   protected void setSFFieldValue(ClientSessionFactoryInternal sf, String tcName, Object value) throws NoSuchFieldException, IllegalAccessException {
      Field tcField = ClientSessionFactoryImpl.class.getDeclaredField(tcName);
      tcField.setAccessible(true);
      tcField.set(sf, value);
   }

   protected TransportConfiguration getFieldFromSF(ClientSessionFactoryInternal sf, String tcName) throws NoSuchFieldException, IllegalAccessException {
      Field tcField = ClientSessionFactoryImpl.class.getDeclaredField(tcName);
      tcField.setAccessible(true);
      return (TransportConfiguration) tcField.get(sf);
   }

   /**
    * Basic fail-back test.
    *
    * @throws Exception
    */
   @Test
   @Timeout(120)
   public void testFailBack() throws Exception {
      boolean doFailBack = true;
      HAPolicy haPolicy = backupServer.getServer().getHAPolicy();
      if (haPolicy instanceof ReplicaPolicy) {
         ((ReplicaPolicy) haPolicy).setMaxSavedReplicatedJournalsSize(1);
      }

      simpleFailover(haPolicy instanceof ReplicaPolicy || haPolicy instanceof ReplicationBackupPolicy, doFailBack);
   }

   @Test
   @Timeout(120)
   public void testSimpleFailover() throws Exception {
      HAPolicy haPolicy = backupServer.getServer().getHAPolicy();

      simpleFailover(haPolicy instanceof ReplicaPolicy || haPolicy instanceof ReplicationBackupPolicy, false);
   }

   @Test
   @Timeout(120)
   public void testWithoutUsingTheBackup() throws Exception {
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      backupServer.stop(); // Backup stops!
      backupServer.start();

      waitForRemoteBackupSynchronization(backupServer.getServer());

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();
      session.commit();

      session.start();
      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      backupServer.stop(); // Backup stops!
      beforeRestart(backupServer);
      backupServer.start();
      waitForRemoteBackupSynchronization(backupServer.getServer());
      backupServer.stop(); // Backup stops!

      primaryServer.stop();
      beforeRestart(primaryServer);
      primaryServer.start();
      primaryServer.getServer().waitForActivation(10, TimeUnit.SECONDS);

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param doFailBack
    * @throws Exception
    */
   private void simpleFailover(boolean isReplicated, boolean doFailBack) throws Exception {
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      SimpleString primaryId = primaryServer.getServer().getNodeID();
      crash(session);

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();

      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      assertEquals(primaryId, backupServer.getServer().getNodeID(), "backup must be running with the same nodeID");
      if (doFailBack) {
         assertFalse(primaryServer.getServer().getHAPolicy().isBackup(), "must NOT be a backup");
         adaptPrimaryConfigForReplicatedFailBack(primaryServer);
         beforeRestart(primaryServer);
         primaryServer.start();
         assertTrue(primaryServer.getServer().waitForActivation(40, TimeUnit.SECONDS), "primary initialized...");
         if (isReplicated) {
            // wait until it switch role again
            Wait.assertTrue(() -> backupServer.getServer().getHAPolicy().isBackup());
            // wait until started
            Wait.assertTrue(backupServer::isStarted);
            // wait until is an in-sync replica
            Wait.assertTrue(backupServer.getServer()::isReplicaSync);
         } else {
            Wait.assertTrue(backupServer::isStarted);
            backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS);
            assertTrue(backupServer.isStarted());
         }
         if (isReplicated) {
            FileMoveManager moveManager = new FileMoveManager(backupServer.getServer().getConfiguration().getJournalLocation(), 0);
            // backup has not had a chance to restart as a backup and cleanup
            Wait.assertTrue(() -> moveManager.getNumberOfFolders() <= 2);
         }
      } else {
         backupServer.stop();
         beforeRestart(backupServer);
         backupServer.start();
         assertTrue(backupServer.getServer().waitForActivation(10, TimeUnit.SECONDS));
      }

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param consumer
    * @throws ActiveMQException
    */
   protected void assertNoMoreMessages(ClientConsumer consumer) throws ActiveMQException {
      ClientMessage msg = consumer.receiveImmediate();
      assertNull(msg, "there should be no more messages to receive! " + msg);
   }

   protected void createSessionFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(300).setRetryInterval(100);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);
   }
   /**
    * @return
    * @throws Exception
    */
   protected ClientSession createSessionAndQueue() throws Exception {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(QueueConfiguration.of(FailoverTestBase.ADDRESS));
      return session;
   }

   protected void sendMessagesSomeDurable(ClientSession session, ClientProducer producer) throws Exception {
      for (int i = 0; i < NUM_MESSAGES; i++) {
         // some are durable, some are not!
         producer.send(createMessage(session, i, isDurable(i)));
      }
   }

   protected void receiveDurableMessages(ClientConsumer consumer) throws ActiveMQException {
      // During failover non-persistent messages may disappear but in certain cases they may survive.
      // For that reason the test is validating all the messages but being permissive with non-persistent messages
      // The test will just ack any non-persistent message, however when arriving it must be in order
      ClientMessage repeatMessage = null;
      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message;

         if (repeatMessage != null) {
            message = repeatMessage;
            repeatMessage = null;
         } else {
            message = consumer.receive(50);
         }

         if (message != null) {
            int msgInternalCounter = message.getIntProperty("counter").intValue();

            if (msgInternalCounter == i + 1) {
               // The test can only jump to the next message if the current iteration is meant for non-durable
               assertFalse(isDurable(i), "a message on counter=" + i + " was expected");
               // message belongs to the next iteration.. let's just ignore it
               repeatMessage = message;
               continue;
            }
         }

         if (isDurable(i)) {
            assertNotNull(message);
         }

         if (message != null) {
            assertMessageBody(i, message);
            assertEquals(i, message.getIntProperty("counter").intValue());
            message.acknowledge();
         }
      }
   }

   protected boolean isDurable(int i) {
      return i % 2 == 0;
   }

   protected void receiveMessages(ClientConsumer consumer) throws ActiveMQException {
      receiveMessages(consumer, 0, NUM_MESSAGES, true);
   }

   @Test
   @Timeout(120)
   public void testSimpleSendAfterFailoverDurableTemporary() throws Exception {
      doSimpleSendAfterFailover(true, true);
   }

   @Test
   @Timeout(120)
   public void testSimpleSendAfterFailoverNonDurableTemporary() throws Exception {
      doSimpleSendAfterFailover(false, true);
   }

   @Test
   @Timeout(120)
   public void testSimpleSendAfterFailoverDurableNonTemporary() throws Exception {
      doSimpleSendAfterFailover(true, false);
   }

   @Test
   @Timeout(120)
   public void testSimpleSendAfterFailoverNonDurableNonTemporary() throws Exception {
      doSimpleSendAfterFailover(false, false);
   }

   private void doSimpleSendAfterFailover(final boolean durable, final boolean temporary) throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(300).setRetryInterval(100);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(QueueConfiguration.of(FailoverTestBase.ADDRESS).setDurable(durable && !temporary).setTemporary(temporary));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      crash(session);

      sendMessagesSomeDurable(session, producer);

      receiveMessages(consumer);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean primary) {
      return TransportConfigurationUtils.getInVMAcceptor(primary);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean primary) {
      return TransportConfigurationUtils.getInVMConnector(primary);
   }

   protected void beforeRestart(TestableServer primaryServer1) {
      // no-op
   }

   protected void decrementActivationSequenceForForceRestartOf(TestableServer primaryServer) throws Exception {
      // no-op
   }

   protected ClientSession sendAndConsume(final ClientSessionFactory sf1, final boolean createQueue) throws Exception {
      ClientSession session = createSession(sf1, false, true, true);

      if (createQueue) {
         session.createQueue(QueueConfiguration.of(FailoverTestBase.ADDRESS).setDurable(false));
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
         message.putIntProperty(SimpleString.of("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBodyBuffer().readString());

         assertEquals(i, message2.getObjectProperty(SimpleString.of("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      assertNull(message3);

      return session;
   }


}
