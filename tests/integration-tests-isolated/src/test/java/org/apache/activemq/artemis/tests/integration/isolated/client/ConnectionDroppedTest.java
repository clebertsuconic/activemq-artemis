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

package org.apache.activemq.artemis.tests.integration.isolated.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerSessionPlugin;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.TcpProxy;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.StringPrintStream;
import org.apache.activemq.artemis.utils.TableOut;
import org.apache.activemq.artemis.utils.ThreadDumpUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionDroppedTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public ConnectionDroppedTest() {
      // this is the reason why I'm putting this test on the "isolated" package.
      disableCheckThread();
   }

   @Test(timeout = 20_000)
   public void testConsumerDroppedWithProtonTestClient() throws Exception {
      int NUMBER_OF_CONNECTIONS = 100;
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();
      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(NUMBER_OF_CONNECTIONS);
      AtomicInteger errors = new AtomicInteger(0);

      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         executorService.execute(() -> {
            try (ProtonTestClient peer = new ProtonTestClient()) {
               peer.queueClientSaslAnonymousConnect();
               peer.remoteOpen().queue();
               peer.expectOpen();
               peer.remoteBegin().queue();
               peer.expectBegin();
               peer.remoteAttach().ofReceiver().withName(RandomUtil.randomString()).withSenderSettleModeUnsettled().withReceivervSettlesFirst().withTarget().also().withSource().withAddress("test-queue").withExpiryPolicyOnLinkDetach().withDurabilityOfNone().withCapabilities("queue").withOutcomes("amqp:accepted:list", "amqp:rejected:list").also().queue();
               peer.dropAfterLastHandler(1000); // This closes the netty connection after the attach is written
               peer.connect("localhost", 61616);

               // Waits for all the commands to fire and the drop action to be run.
               peer.waitForScriptToComplete();
            } catch (Throwable e) {
               errors.incrementAndGet();
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         });
      }

      Assert.assertTrue(done.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(0, errors.get());

      Wait.assertEquals(0, () -> serverQueue.getConsumers().size(), 5000, 100);
   }

   @Test(timeout = 20_000)
   public void testRegularClose() throws Exception {
      int NUMBER_OF_CONNECTIONS = 100;
      int REPEATS = 10;
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();
      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(NUMBER_OF_CONNECTIONS);
      AtomicInteger errors = new AtomicInteger(0);
      AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler();
      runAfter(loggerHandler::stop);

      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         executorService.execute(() -> {
            for (int r = 0; r < REPEATS; r++) {
               try (ProtonTestClient peer = new ProtonTestClient()) {
                  peer.queueClientSaslAnonymousConnect();
                  peer.remoteOpen().queue();
                  peer.expectOpen();
                  peer.remoteBegin().queue();
                  peer.expectBegin();
                  peer.remoteAttach().ofReceiver().withName(RandomUtil.randomString()).withSenderSettleModeUnsettled().withReceivervSettlesFirst().withTarget().also().withSource().withAddress("test-queue").withExpiryPolicyOnLinkDetach().withDurabilityOfNone().withCapabilities("queue").withOutcomes("amqp:accepted:list", "amqp:rejected:list").also().queue();
                  peer.expectAttach();
                  peer.remoteClose().queue();
                  peer.expectClose();

                  peer.connect("localhost", 61616);

                  peer.waitForScriptToComplete();
               } catch (Throwable e) {
                  errors.incrementAndGet();
                  logger.warn(e.getMessage(), e);
                  break;
               }
            }
            done.countDown();
         });
      }

      Assert.assertTrue(done.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(0, errors.get());

      Assert.assertFalse(loggerHandler.findText("AMQ212037"));

      Assert.assertFalse(loggerHandler.findText("Connection failure"));
      Assert.assertFalse(loggerHandler.findText("REMOTE_DISCONNECT"));
      Assert.assertFalse(loggerHandler.findText("AMQ222061"));
      Assert.assertFalse(loggerHandler.findText("AMQ222107"));

      Wait.assertEquals(0, () -> serverQueue.getConsumers().size(), 5000, 100);
      Wait.assertEquals(0, server::getConnectionCount, 5000);

   }

   @Test
   public void testConsumerDroppedAMQP() throws Throwable {
      testConsumerDroppedWithRegularClient("AMQP");

   }

   @Test
   public void testConsumerDroppedCORE() throws Throwable {
      testConsumerDroppedWithRegularClient("CORE");
   }

   @Test
   public void testConsumerDroppedOpenWire() throws Throwable {
      testConsumerDroppedWithRegularClient("OPENWIRE");
   }

   public void testConsumerDroppedWithRegularClient(final String protocol) throws Throwable {
      int NUMBER_OF_CONNECTIONS = 25;
      int REPEATS = 10;
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();
      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(NUMBER_OF_CONNECTIONS);

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final AtomicBoolean running = new AtomicBoolean(true);

      runAfter(() -> running.set(false));

      CyclicBarrier flagStart = new CyclicBarrier(NUMBER_OF_CONNECTIONS + 1);
      flagStart.reset();

      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         final int t = i;
         executorService.execute(() -> {
            try {
               boolean alreadyStarted = false;
               AtomicBoolean ex = new AtomicBoolean(true);
               while (running.get()) {
                  try {
                     // do not be tempted to use try (connection = factory.createConnection())
                     // this is because we don't need to close the connection after a network failure on this test.
                     Connection connection = factory.createConnection();

                     synchronized (ConnectionDroppedTest.this) {
                        runAfter(connection::close);
                     }
                     connection.setExceptionListener(new ExceptionListener() {
                        @Override
                        public void onException(JMSException exception) {
                           ex.set(true);
                        }
                     });
                     flagStart.await(60, TimeUnit.SECONDS);

                     connection.start();

                     Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                     javax.jms.Queue jmsQueue = session.createQueue("test-queue");

                     while (running.get() && !ex.get()) {
                        if (!alreadyStarted) {
                           alreadyStarted = true;
                        }
                        System.out.println("Consumer");
                        MessageConsumer consumer = session.createConsumer(jmsQueue);
                        Thread.sleep(500);
                     }

                     if (!protocol.equals("CORE")) {
                        connection.close();
                     }
                  } catch (Exception e) {
                     logger.debug(e.getMessage(), e);
                  }
               }
            } finally {
               done.countDown();
            }
         });
      }

      for (int i = 0; i < REPEATS; i++) {
         try {
            flagStart.await(60, TimeUnit.SECONDS); // align all the clients at the same spot
         } catch (Throwable throwable) {
            logger.info(ThreadDumpUtil.threadDump("timed out flagstart"));
            throw throwable;
         }

         logger.info("*******************************************************************************************************************************\nloop kill {}" + "\n*******************************************************************************************************************************", i);
         server.getRemotingService().getConnections().forEach(r -> {
            r.fail(new ActiveMQException("it's a simulation"));
         });

      }

      running.set(false);
      try {
         flagStart.await(1, TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }
      if (!done.await(10, TimeUnit.SECONDS)) {
         logger.warn(ThreadDumpUtil.threadDump("Threads are still running"));
         Assert.fail("Threads are still running");
      }

      Wait.assertEquals(0, () -> serverQueue.getConsumers().size(), 5000, 100);

   }

   @Test
   public void testDropConsumerProtonJ2TestClient() throws Throwable {
      ReusableLatch latchCreating = new ReusableLatch(1);
      ReusableLatch blockCreate = new ReusableLatch(1);
      ReusableLatch done = new ReusableLatch(1);
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();

      int TEST_REPEATS = 4;

      server.registerBrokerPlugin(new ActiveMQServerSessionPlugin() {
         @Override
         public void beforeCreateSession(String name,
                                         String username,
                                         int minLargeMessageSize,
                                         RemotingConnection connection,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         boolean preAcknowledge,
                                         boolean xa,
                                         String defaultAddress,
                                         SessionCallback callback,
                                         boolean autoCreateQueues,
                                         OperationContext context,
                                         Map<SimpleString, RoutingType> prefixes) throws ActiveMQException {
            latchCreating.countDown();
            try {
               blockCreate.await(10, TimeUnit.HOURS);
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         }
      });

      AtomicBoolean running = new AtomicBoolean(true);
      ExecutorService executorService = Executors.newFixedThreadPool(1);
      runAfter(executorService::shutdownNow);
      runAfter(() -> running.set(false));
      Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setAddress(getName()).setAutoCreated(false));

      for (int i = 0; i < TEST_REPEATS; i++) {
         Assert.assertEquals(0, serverQueue.getConsumerCount());
         latchCreating.setCount(1);
         blockCreate.setCount(1);
         done.setCount(1);

         ProtonTestClient peer = new ProtonTestClient();

         executorService.execute(() -> {

            try {
               peer.queueClientSaslAnonymousConnect();
               peer.remoteOpen().queue();
               peer.remoteBegin().queue();
               peer.remoteAttach().ofReceiver().withName(RandomUtil.randomString()).withSenderSettleModeUnsettled().withReceivervSettlesFirst().withTarget().also().withSource().withAddress(getName()).withExpiryPolicyOnLinkDetach().withDurabilityOfNone().withCapabilities("queue").withOutcomes("amqp:accepted:list", "amqp:rejected:list").also().queue();

               peer.connect("localhost", 61616);

               peer.waitForScriptToCompleteIgnoreErrors();
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
            }
         });

         Assert.assertTrue(latchCreating.await(10, TimeUnit.SECONDS));
         server.getRemotingService().getConnections().forEach(r -> {
            r.fail(new ActiveMQException("it's a simulation"));
         });
         blockCreate.countDown();
         Assert.assertTrue(done.await(10, TimeUnit.SECONDS));

         { // double checking the executor is done
            CountDownLatch check = new CountDownLatch(1);
            executorService.execute(check::countDown);
            Assert.assertTrue(check.await(1, TimeUnit.SECONDS));
         }

         Thread.sleep(100); // I need some time for the error condition to kick in

         Wait.assertEquals(0, server::getConnectionCount, 5000);

         Wait.assertEquals(0, serverQueue::getConsumerCount, 5000);
      }

   }

   @Test
   public void testDropCreateConsumerAMQP() throws Throwable {
      testDropCreateConsumer("AMQP");
   }

   @Test
   public void testDropCreateConsumerOPENWIRE() throws Throwable {
      testDropCreateConsumer("OPENWIRE");
   }

   @Test
   public void testDropCreateConsumerCORE() throws Throwable {
      testDropCreateConsumer("CORE");
   }

   private void testDropCreateConsumer(String protocol) throws Throwable {
      CountDownLatch latchCreating = new CountDownLatch(1);
      CountDownLatch blockCreate = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(1);
      CountDownLatch doneConsumer = new CountDownLatch(1);
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();

      Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setAddress(getName()).setAutoCreated(false));

      AtomicBoolean running = new AtomicBoolean(true);
      ExecutorService executorService = Executors.newFixedThreadPool(1);
      runAfter(executorService::shutdownNow);
      runAfter(() -> running.set(false));

      CountDownLatch received = new CountDownLatch(1);

      executorService.execute(() -> {
         ConnectionFactory connectionFactory;

         switch (protocol) {
            case "AMQP":
               connectionFactory = CFUtil.createConnectionFactory(protocol, "failover:(amqp://localhost:61616)?failover.maxReconnectAttempts=20");
               break;
            case "OPENWIRE":
               connectionFactory = new org.apache.activemq.ActiveMQConnectionFactory( "failover:(tcp://localhost:61616)?maxReconnectAttempts=20");
               break;
            case "CORE":
               connectionFactory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:61616?ha=true;reconnectAttempts=20;callTimeout=1000");
               break;
            default:
               logger.warn("Invalid protocol {}", protocol);
               connectionFactory = null;
         }

         try (Connection connection = connectionFactory.createConnection()) {
            logger.info("Connected on thread..");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(getName()));
            connection.start();
            while (running.get()) {
               try {
                  logger.info("Receiving");
                  received.countDown();
                  Message message = consumer.receive(100);
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            }
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         } finally {
            doneConsumer.countDown();
         }
      });

      Assert.assertTrue(received.await(10, TimeUnit.SECONDS));

      server.registerBrokerPlugin(new ActiveMQServerSessionPlugin() {
         @Override
         public void beforeCreateSession(String name,
                                         String username,
                                         int minLargeMessageSize,
                                         RemotingConnection connection,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         boolean preAcknowledge,
                                         boolean xa,
                                         String defaultAddress,
                                         SessionCallback callback,
                                         boolean autoCreateQueues,
                                         OperationContext context,
                                         Map<SimpleString, RoutingType> prefixes) throws ActiveMQException {
            latchCreating.countDown();
            try {
               blockCreate.await(10, TimeUnit.HOURS);
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         }
      });


      server.getRemotingService().getConnections().forEach(r -> {
         r.fail(new ActiveMQException("it's a simulation"));
      });
      Assert.assertTrue(latchCreating.await(10, TimeUnit.SECONDS));
      server.getRemotingService().getConnections().forEach(r -> {
         r.fail(new ActiveMQException("it's a simulation 2nd time"));
      });
      blockCreate.countDown();
      Assert.assertTrue(done.await(10, TimeUnit.SECONDS));

      running.set(false);
      Assert.assertTrue(doneConsumer.await(40, TimeUnit.SECONDS));

      Thread.sleep(100);

      { // double checking the executor is done
         CountDownLatch check = new CountDownLatch(1);
         executorService.execute(check::countDown);
         Assert.assertTrue(check.await(1, TimeUnit.SECONDS));
      }

      Wait.assertEquals(0, server::getConnectionCount, 5000);
      Wait.assertEquals(0, serverQueue::getConsumerCount, 5000);

   }


   @Test(timeout = 10_000)
   public void testForceDropOpenWire() throws Throwable {
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();

      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      CountDownLatch beforeCreateCalled = new CountDownLatch(1);
      CountDownLatch goCreateConsumer = new CountDownLatch(1);
      server.registerBrokerPlugin(new ActiveMQServerConsumerPlugin() {
         @Override
         public void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {
            if (consumer.getQueue() == serverQueue) {
               logger.info("Creating a consumer at {}", consumer.getQueue());
               beforeCreateCalled.countDown();
               try {
                  goCreateConsumer.await(5, TimeUnit.MINUTES);
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            }
         }
      });

      ExecutorService executorService = Executors.newFixedThreadPool(1);
      runAfter(executorService::shutdownNow);

      ConnectionFactory factory = CFUtil.createConnectionFactory("OPENWIRE", "tcp://localhost:61616");

      CountDownLatch done = new CountDownLatch(1);

      executorService.execute(() -> {
         try (Connection connection = factory.createConnection();
              Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
              MessageConsumer consumer = session.createConsumer(session.createQueue("test-queue"))) {
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         } finally {
            done.countDown();
         }
      });

      Assert.assertTrue(beforeCreateCalled.await(5, TimeUnit.MINUTES));

      server.getRemotingService().getConnections().forEach(r -> {
         r.fail(new ActiveMQException("this is a simulation"));
      });

      goCreateConsumer.countDown();

      Wait.assertEquals(0, serverQueue::getConsumerCount);
   }

   // Reproduces a scenario where the connection cleanup is stuck on a blocked event loop,
   // preventing the container-id from being released and blocking reconnection.
   @Test(timeout = 60_000)
   public void testGhostConnectionBlockedEventLoop() throws Throwable {
      final int PROXY_PORT = 61617;
      final String CLIENT_ID = "unique-client-test";
      final long TTL = 1000;
      final long TTL_CHECK_INTERVAL = 100;

      Configuration config = createDefaultConfig(true);
      config.setConnectionTTLOverride(TTL);
      config.setConnectionTtlCheckInterval(TTL_CHECK_INTERVAL);

      ActiveMQServer server = createServer(true, config);
      server.start();

      TcpProxy proxy = new TcpProxy("localhost", 61616, PROXY_PORT, false);
      proxy.startProxy();
      runAfter(() -> proxy.stopProxy());

      ConnectionFactory factoryThroughProxyNoRetries = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + PROXY_PORT);

      // Step 1: connect through the proxy with a fixed clientID
      Connection connection1 = factoryThroughProxyNoRetries.createConnection();
      connection1.setClientID(CLIENT_ID);
      connection1.start();

      Wait.assertEquals(1, server::getConnectionCount, 5000);

      logger.info("trying to connect...");
      try {
         Connection duplicateConnection = factoryThroughProxyNoRetries.createConnection();
         duplicateConnection.setClientID(CLIENT_ID);
         duplicateConnection.close();
         Assert.fail("Should have thrown an exception for duplicate clientID");
      } catch (JMSException e) {
         logger.info("Duplicate clientID correctly rejected while first connection alive: {}", e.getMessage());
      }

      logger.info("Connected....");

      // Step 2: block the netty event loop that owns this connection
      CountDownLatch blockLatch = new CountDownLatch(1);
      CountDownLatch eventLoopBlocked = new CountDownLatch(1);
      runAfter(blockLatch::countDown);

      RemotingConnection remotingConn = server.getRemotingService().getConnections().iterator().next();
      EventLoop eventLoop = remotingConn.getTransportConnection().getEventLoop();
      eventLoop.execute(() -> {
         eventLoopBlocked.countDown();
         try {
            blockLatch.await(30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      });

      logger.info("ConnectionList on the beggining\n{}", listConnection(server));

      Assert.assertTrue("Event loop should be blocked", eventLoopBlocked.await(5, TimeUnit.SECONDS));

      // Step 3: stop proxy traffic so no idle frames flow, letting TTL expire.
      // The cleanup is queued to the blocked event loop, so it won't run.
      proxy.stopAllHandlers();

      // Wait for TTL + check intervals to fire and call fail()
      Thread.sleep(TTL + TTL_CHECK_INTERVAL * 3);

      // Step 5: resume the event loop, the queued cleanup should run and release the clientID
      blockLatch.countDown();

      // Restart the proxy and reconnect, both on background threads
      ExecutorService auxiliaryExecutor = Executors.newFixedThreadPool(5);
      runAfter(auxiliaryExecutor::shutdownNow);

      AtomicBoolean running = new AtomicBoolean(true);
      runAfter(() -> running.set(true));

      proxy.stopProxy();
      TcpProxy proxy2 = new TcpProxy("localhost", 61616, PROXY_PORT, false);
      auxiliaryExecutor.execute(() -> {
         try {
            Thread.sleep(500);
         } catch (Throwable ignored) {
         }
         proxy2.startProxy();
      });
      runAfter(() -> proxy2.stopProxy());

      auxiliaryExecutor.execute(() -> {
         while (running.get()) {
            try {
               Thread.sleep(1000);
            } catch (Exception ignored) {
            }

            try {
               String connectionList = listConnection(server);
               logger.info("\n{}", connectionList);
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            }
         }
      });

      ConnectionFactory factoryThroughProxyRetries = CFUtil.createConnectionFactory("AMQP", "failover:(amqp://localhost:" + PROXY_PORT + ")?failover.maxReconnectAttempts=300&failover.reconnectDelay=100");

      // Step 6: now the clientID should be available again
      try {
         auxiliaryExecutor.submit(() -> {
            try (Connection connection3 = factoryThroughProxyRetries.createConnection()) {
               connection3.setClientID(CLIENT_ID);
               connection3.start();
            }
            return null;
         }).get(30, TimeUnit.SECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         Assert.fail("Reconnection with same clientID did not complete within 30 seconds");
      }

      Wait.assertEquals(0, server::getConnectionCount, 5000);

      try {
         connection1.close();
      } catch (Exception ignored) {
      }
   }

   private static String listConnection(ActiveMQServer server) throws IOException {
      TableOut tableOut = new TableOut("|", 0, new int[]{40, 25, 10});
      StringPrintStream stringPrintStream = new StringPrintStream();
      java.io.PrintStream printStream = stringPrintStream.newStream();
      printStream.println("Active connections (count=" + server.getConnectionCount() + "):");
      tableOut.print(printStream, new String[]{"ID", "ClientID", "Destroyed"});
      tableOut.printSeparator(printStream);
      server.getRemotingService().getConnections().forEach(conn -> {
         tableOut.print(printStream, new String[]{String.valueOf(conn.getID()), String.valueOf(conn.getClientID()), String.valueOf(conn.isDestroyed())});
      });
      return stringPrintStream.toString();
   }

}
