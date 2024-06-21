package org.apache.activemq.artemis.tests.integration.cluster.bridge;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStorePrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class MessageCount235Test extends FailoverTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private Map<String, Object> dc2Primary1Conf;
   private TransportConfiguration dc2Primary1tc;

   private static int MAX_SIZE_BYTES_IN_KB = 400;

   // Topology: Simulating DC1 (1 primary + backup pair)
   // Simulating DC2 1 single primary node

   // 1. Produce messages on dc1Primary1
   // 2. Start a bridge from dc1Primary1 to dc2Primary1
   // 3. Stop dc1Primary1 while transfer has started but not finished
   // 4. dc1Backup1 become alive
   // 5. Start a bridge from dc1Backup1 to dc2Primary1
   // 6. dc2Primary1 has received all messages produced originally
   // 7. Expected: messageCount on dc1Backup1 to be equal to 0, but actual is > 0
   //
   // Notes: if address is not paging, there is no issue
   @Test
   public void testMessageCount_whenFailoverHappenWhileBridgeIsOpen() throws Exception {
      ActiveMQServer dc1Primary1 = primaryServer.getServer();
      ActiveMQServer dc1Backup1 = backupServer.getServer();
      ActiveMQServer dc2Primary1 = createClusteredServerWithParams(true, 3, true, dc2Primary1Conf);
      final String bridgeName = "bridge1";
      final String queueName0 = "queue0";
      logger.info("Starting remote cluster node  dc2Primary1");
      dc2Primary1.start();
      waitForServerToStart(dc2Primary1);
      try {
         Queue queue = dc2Primary1.createQueue(QueueConfiguration.of(queueName0).setDurable(true));

         int messageSent = 2000;
         logger.info("Producing {} messages ", messageSent);
         try (ClientSessionFactory sessionFactory = createSessionFactory(getServerLocator())) {
            ClientSession session1 = sessionFactory.createSession(false, false);
            QueueConfiguration queueConfiguration = QueueConfiguration.of(queueName0).setDurable(true);
            session1.createQueue(queueConfiguration);
            ClientProducer producerServer1 = session1.createProducer(queueName0);
            for (int i = 0; i < messageSent; i++) {
               ClientMessage msg = session1.createMessage(true);
               if (i % 2 == 0) setLargeMessageBody(0, msg);
               else setBody(0, msg);
               producerServer1.send(msg);
            }
            session1.commit();
            producerServer1.close();
            session1.close();
         }

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName)
            .setQueueName(queueName0).setRetryInterval(1000)
            .setUseDuplicateDetection(true)
            .setStaticConnectors(List.of(dc2Primary1tc.getName()));

         logger.info("Starting bridge: " + bridgeConfiguration.getName());
         dc1Primary1.getClusterManager().deployBridge(bridgeConfiguration);

         Wait.waitFor(() -> messageSent - getMessageCount(dc1Primary1, queueName0) > 100);
         logger.info("Stopping dc1Primary1");
         dc1Primary1.stop();
         logger.info("Waiting for dc1Backup1 to be alive");
         Wait.waitFor(dc1Backup1::isActive, 15000);
         logger.info("dc1Backup1 isAlive");
         long messageCountNodeDC1 = getMessageCount(dc1Backup1, queueName0);
         long messageCountNodeDC2 = getMessageCount(dc2Primary1, queueName0);
         Thread.sleep(5000);
         logger.info("Count in post office: " + (messageCountNodeDC1 + messageCountNodeDC2) + "messageCountNodeDC1:" + messageCountNodeDC1 + ", messageCountNodeDC2:" + messageCountNodeDC2 + " should be equal to: " + messageSent);
         dc1Backup1.getClusterManager().deployBridge(bridgeConfiguration);
         Wait.waitFor(() -> getMessageCount(dc2Primary1, queueName0) == messageSent);
         messageCountNodeDC1 = getMessageCount(dc1Backup1, queueName0);
         messageCountNodeDC2 = getMessageCount(dc2Primary1, queueName0);
         logger.info("Count in post office: " + (messageCountNodeDC1 + messageCountNodeDC2) + "messageCountNodeDC1:" + messageCountNodeDC1 + ", messageCountNodeDC2:" + messageCountNodeDC2 + " should be equal to: " + messageSent);
         Assertions.assertEquals(messageSent, messageCountNodeDC2);
         HashSet<Long> longConters = new HashSet<>();
         System.out.println("" + dc1Backup1.getConfiguration().getBindingsDirectory());
         System.out.println("journal: " + dc1Backup1.getConfiguration().getJournalDirectory());
         System.out.println("paging: " + dc1Backup1.getConfiguration().getPagingDirectory());

         for (int i = 0; i < 10; i++) {
            System.out.println("Message Count :: " + getMessageCount(dc1Backup1, queueName0));
            Queue queue0 = dc1Backup1.locateQueue(queueName0);
            queue0.getPagingStore().forceAnotherPage();
            Future<Boolean> scheduledCleanup = queue0.getPagingStore().getCursorProvider().scheduleCleanup();
            scheduledCleanup.get();
            Future future = dc1Backup1.getPagingManager().rebuildCounters(new HashSet<>());
            future.get();
         }
         Wait.assertEquals(0, () -> getMessageCount(dc1Backup1, queueName0), 5000, 1000);
      } finally {
         dc2Primary1.stop();
      }
   }

   @Test
   public void testMessageCount_whenFailoverHappenWhileMessageAreConsumed() throws Exception {
      ActiveMQServer dc1Primary1 = primaryServer.getServer();
      ActiveMQServer dc1Backup1 = backupServer.getServer();
      final String queueName0 = "queue0";

      int messageSent = 2000;
      logger.info("Producing {} messages ", messageSent);
      try (ClientSessionFactory sessionFactory = createSessionFactory(getServerLocator())) {
         ClientSession session1 = sessionFactory.createSession(false, false);
         QueueConfiguration queueConfiguration = QueueConfiguration.of(queueName0).setDurable(true);
         session1.createQueue(queueConfiguration);
         ClientProducer producerServer1 = session1.createProducer(queueName0);
         for (int i = 0; i < messageSent; i++) {
            ClientMessage msg = session1.createMessage(true);
            if (i % 2 == 0) setLargeMessageBody(0, msg);
            else setBody(0, msg);
            producerServer1.send(msg);
         }
         session1.commit();
         producerServer1.close();
         session1.close();
      }

      CountDownLatch acknowledgeLatch = new CountDownLatch(1);
      CountDownLatch commitLatch = new CountDownLatch(1);
      CountDownLatch dc1Primary1StoppedLatch = new CountDownLatch(1);
      CompletableFuture.runAsync(() -> {
         try (ClientSessionFactory sessionFactory = createSessionFactory(getServerLocator())) {
            ClientSession session1 = sessionFactory.createSession(false, false);
            session1.start();
            ClientConsumer consumer = session1.createConsumer(queueName0);

            ClientMessage receive = consumer.receive();
            receive.acknowledge();
            acknowledgeLatch.countDown();
            dc1Primary1StoppedLatch.await();
            session1.commit();
            session1.close();
            consumer.close();
         } catch (Exception e) {
            e.printStackTrace();
         } finally {
            commitLatch.countDown();
         }
      });
      long messageCountNodeDC1 = getMessageCount(dc1Primary1, queueName0);
      logger.info("Count in post office: dc1Primary1: {}", messageCountNodeDC1);

      acknowledgeLatch.await();
      logger.info("Stopping dc1Primary1");
      dc1Primary1.stop();
      dc1Primary1StoppedLatch.countDown();
      logger.info("Waiting for dc1Backup1 to be alive");
      Wait.waitFor(dc1Backup1::isActive, 15000);
      logger.info("dc1Backup1 isAlive");
      commitLatch.await();
      messageCountNodeDC1 = getMessageCount(dc1Backup1, queueName0);
      logger.info("Count in post office: dc1Backup1: {}", messageCountNodeDC1);
      Assertions.assertEquals(messageSent, messageCountNodeDC1);
   }

   @Test
   public void testMessageCount_whenFailoverHappenAfterMessageAreConsumed() throws Exception {
      ActiveMQServer dc1Primary1 = primaryServer.getServer();
      ActiveMQServer dc1Backup1 = backupServer.getServer();
      final String queueName0 = "queue0";

      int messageSent = 2000;
      logger.info("Producing {} messages ", messageSent);
      try (ClientSessionFactory sessionFactory = createSessionFactory(getServerLocator())) {
         ClientSession session1 = sessionFactory.createSession(false, false);
         QueueConfiguration queueConfiguration = QueueConfiguration.of(queueName0).setDurable(true);
         session1.createQueue(queueConfiguration);
         ClientProducer producerServer1 = session1.createProducer(queueName0);
         for (int i = 0; i < messageSent; i++) {
            ClientMessage msg = session1.createMessage(true);
            if (i % 2 == 0) setLargeMessageBody(0, msg);
            else setBody(0, msg);
            producerServer1.send(msg);
         }
         session1.commit();
         producerServer1.close();
         session1.close();
      }

      int messagesToConsume = 156;
      logger.info("Consuming {} messages", messagesToConsume);
      try (ClientSessionFactory sessionFactory = createSessionFactory(getServerLocator())) {
         ClientSession session1 = sessionFactory.createSession(false, false);
         session1.start();
         ClientConsumer consumer = session1.createConsumer(queueName0);
         for(int i = 0; i < messagesToConsume; i++) {
            ClientMessage receive = consumer.receive();
            receive.acknowledge();
         }
         session1.commit();
         session1.close();
         consumer.close();
      }
      long messageCountNodeDC1 = getMessageCount(dc1Primary1, queueName0);
      logger.info("Count in post office: dc1Primary1: {}", messageCountNodeDC1);

      logger.info("Stopping dc1Primary1");
      dc1Primary1.stop();
      logger.info("Waiting for dc1Backup1 to be alive");
      Wait.waitFor(dc1Backup1::isActive, 15000);
      logger.info("dc1Backup1 isAlive");
      messageCountNodeDC1 = getMessageCount(dc1Backup1, queueName0);
      logger.info("Count in post office: dc1Backup1: {}", messageCountNodeDC1);
      Assertions.assertEquals(messageSent - messagesToConsume, messageCountNodeDC1);
   }


   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean primary) {
      return TransportConfigurationUtils.getInVMAcceptor(primary);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean primary) {
      return TransportConfigurationUtils.getInVMConnector(primary);
   }


   @Override
   protected void createConfigs() throws Exception {
      dc2Primary1Conf = new HashMap<>();
      dc2Primary1Conf.put("host", TransportConstants.DEFAULT_HOST);
      dc2Primary1Conf.put("port", TransportConstants.DEFAULT_PORT + 3);
      dc2Primary1tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, dc2Primary1Conf);
      nodeManager = new InVMNodeManager(false);
      TransportConfiguration primaryConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig = super.createDefaultConfig(true).setPersistenceEnabled(true)
         .clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(false))
         .setHAPolicyConfiguration(new SharedStoreBackupPolicyConfiguration().setScaleDownConfiguration(new ScaleDownConfiguration().setEnabled(false)).setRestartBackup(false))
         .addConnectorConfiguration(primaryConnector.getName(), primaryConnector).addConnectorConfiguration(backupConnector.getName(), backupConnector)
         .addConnectorConfiguration(dc2Primary1tc.getName(), dc2Primary1tc).addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), primaryConnector.getName()))
         .addAddressSetting("#", new AddressSettings().setMaxSizeBytes(MAX_SIZE_BYTES_IN_KB * 1024L));

      backupServer = createTestableServer(backupConfig);

      primaryConfig = super.createDefaultConfig(true).setPersistenceEnabled(true)
         .clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
         .setHAPolicyConfiguration(new SharedStorePrimaryPolicyConfiguration().setFailoverOnServerShutdown(true))
         .addClusterConfiguration(basicClusterConnectionConfig(primaryConnector.getName())).addConnectorConfiguration(dc2Primary1tc.getName(), dc2Primary1tc)
         .addConnectorConfiguration(primaryConnector.getName(), primaryConnector)
         .addAddressSetting("#", new AddressSettings().setMaxSizeBytes(MAX_SIZE_BYTES_IN_KB * 1024L));

      primaryServer = createTestableServer(primaryConfig);
   }
}