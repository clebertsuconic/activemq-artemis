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

package org.apache.activemq.artemis.tests.soak.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OscillateReplicaPagingTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String propertyIdentification = "osc_I";


   private static final boolean USE_KILL = false;

   private static final String BODY_STRING = "hello " + "a".repeat(1_000);

   public OscillateReplicaPagingTest() {
   }

   public static final String SERVER_NAME_0 = "oscillate-server0";
   public static final String SERVER_NAME_1 = "oscillate-server1";

   AtomicInteger produced = new AtomicInteger(0);
   AtomicInteger consumed = new AtomicInteger(0);

   ConcurrentHashMap<Integer, Integer> receivedMessages = new ConcurrentHashMap<>();

   private static final int KEEP_RUNNING = 100_000;
   private static final int RESTART_REPLICA_INTERVAL = 10_000;
   private static final int NRESTARTS = 1;

   private Process serverLive;

   private Process serverReplica;

   protected void startLive() throws Exception {
      serverLive = startServer(SERVER_NAME_0, 0, 30000);
   }

   protected void startBackup() throws Exception {
      serverReplica = startServer(SERVER_NAME_1, -1, -1);
   }

   private void waitReplicaSync() throws Exception {
      try (SimpleManagement simpleManagement = new SimpleManagement("tcp://localhost:61616", "guest", "guest")) {
         runAfter(simpleManagement::close);
         Wait.assertTrue(simpleManagement::isReplicaSync);
      }
   }

   private void destroyLive() throws Exception {
      if (USE_KILL) {
         serverLive.destroyForcibly();
      } else {
         stopServerWithFile(getServerLocation(SERVER_NAME_0));
      }
      assertTrue(serverLive.waitFor(1, TimeUnit.MINUTES));
   }

   private void destroyBackup() throws Exception {
      if (USE_KILL) {
         serverReplica.destroyForcibly();
      } else {
         stopServerWithFile(getServerLocation(SERVER_NAME_1));
      }
      assertTrue(serverReplica.waitFor(1, TimeUnit.MINUTES));
   }


   @Before
   public void createServers() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = new HelperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/page-oscillation/replica-0");
         cliCreateServer.createServer();
         configureAddress0(null);
      }
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_1);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = new HelperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/page-oscillation/replica-1");
         cliCreateServer.createServer();
         configureAddress1(null);
      }
   }

   private void configureAddress0(Integer extraQueueID) throws Exception {
      configureAddress(getFileServerLocation(SERVER_NAME_0), "./src/main/resources/servers/page-oscillation/replica-0/broker.xml", extraQueueID);
   }

   private void configureAddress1(Integer extraQueueID) throws Exception {
      configureAddress(getFileServerLocation(SERVER_NAME_1), "./src/main/resources/servers/page-oscillation/replica-1/broker.xml", extraQueueID);
   }

   private void configureAddress(File serverLocation, String originalFile, Integer extraQueueID) throws Exception {

      StringWriter writer = new StringWriter();
      PrintWriter out = new PrintWriter(writer);

      out.println("         <address name=\"" + OSCILLATE_ADDRESS + "\">");
      out.println("           <multicast>");
      out.println("                    <queue name=\"" + OSCILLATE_QUEUE_NAME + "\" purge-on-no-consumers=\"false\"/>");
      if (extraQueueID != null) {
         out.println("                    <queue name=\"" + OSCILLATE_QUEUE_NAME + "_" + extraQueueID + "\" purge-on-no-consumers=\"false\"/>");
      }
      out.println("           </multicast>");
      out.println("         </address>");

      File tempFile = new File(serverLocation, "/etc/temp-xml");
      if (tempFile.exists()) {
         tempFile.delete(); // just in case
      }
      Files.copy(new File(originalFile).toPath(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      assertTrue(FileUtil.findReplace(tempFile, "&&REPLACE-HERE&&", writer.toString()));

      File targetConfig = new File(serverLocation, "/etc/broker.xml");
      Files.copy(tempFile.toPath(), targetConfig.toPath(), StandardCopyOption.REPLACE_EXISTING);
      tempFile.delete();
   }

   @Before
   public void before() throws Exception {
      sequenceGenerator.set(0);
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
   }

   static final String BUSY_QUEUE = "busyQueue";
   static final String OSCILLATE_QUEUE_NAME = "oscillateSubs";
   static final String OSCILLATE_ADDRESS = "oscillateAddress";
   static final String OSCILLATE_QUEUE_EXPRESSION = OSCILLATE_ADDRESS + "::" + OSCILLATE_QUEUE_NAME;

   static final int BROWSERS = 1;
   static final int SPIKE_THREADS = 1;
   static final int OSCILLATE_CONSUMERS = 50;
   static final int  OSCILLATE_PRODUCERS = 10;
   static final int BUSY_PRODUCERS = 1;
   static final int BUSY_CONSUMERS = 1;

   CyclicBarrier barrierOscillation = new CyclicBarrier(OSCILLATE_PRODUCERS);

   AtomicInteger errors = new AtomicInteger(0);
   CountDownLatch done;
   static AtomicInteger sequenceGenerator = new AtomicInteger(0);

   volatile boolean running = true;

   @Test
   public void testOscillateLoad() throws Throwable {
      startLive();
      startBackup();

      waitReplicaSync();

      final int TOTAL_THREADS = OSCILLATE_CONSUMERS + OSCILLATE_PRODUCERS + BUSY_PRODUCERS + BUSY_CONSUMERS + BROWSERS + SPIKE_THREADS;
      done = new CountDownLatch(TOTAL_THREADS);

      ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREADS);
      runAfter(executorService::shutdownNow);

      int threadsUsed = 0;

      for (int i = 0; i < SPIKE_THREADS; i++) {
         executorService.execute(this::oscillateSpikeProducer);
         threadsUsed++;
      }

      // busy consumer and producer are here just guarantee the storage is full of stuff and it's working
      for (int i = 0; i < BUSY_PRODUCERS; i++) {
         executorService.execute(this::busyProducer);
         threadsUsed++;
      }
      for (int i = 0; i < BUSY_CONSUMERS; i++) {
         executorService.execute(this::busyConsumer);
         threadsUsed++;
      }

      for (int i = 0; i < OSCILLATE_CONSUMERS; i++) {
         threadsUsed++;
         executorService.execute(this::oscillateConsumer);
      }

      for (int i = 0; i < OSCILLATE_PRODUCERS; i++) {
         executorService.execute(this::oscillateProducer);
         threadsUsed++;
      }

      for (int i = 0; i < BROWSERS; i++) {
         executorService.execute(this::oscillateBrowser);
         threadsUsed++;
      }

      // this is just to validate I configured it correctly
      assertEquals(threadsUsed, TOTAL_THREADS);

      Thread.sleep(KEEP_RUNNING);
      running = false;
      assertTrue(done.await(100, TimeUnit.SECONDS));
      Thread.sleep(1_000);

      logger.info("this is done... ");

      finalConsume();

      assertTrue(produced.get() > 0);

      if (produced.get() != consumed.get()) {
         logger.info("produced = {}, consumed = {}, receivedMessages size = {}", produced.get(), consumed.get(), receivedMessages.size());
         receivedMessages.forEach((a, b) -> {
            logger.info("Message {} is nowhere to be found", a);
         });
      }

      assertEquals(produced.get(), consumed.get());
      assertEquals(0, errors.get());

      destroyBackup();

      destroyLive();
   }

   @Test
   public void testHoldMessagesAndRemove() throws Exception {
      configureAddress0(100);
      startLive();

      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      factory.setBlockOnDurableSend(false);

      int nMessages = 10_000;

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));

         for (int i = 0; i < nMessages; i++) {
            Message message = session.createTextMessage(BODY_STRING);
            message.setIntProperty("originalI", i);
            producer.send(message);
            if (i % 1000 == 0) {
               session.commit();
               logger.info("Sending {}", i);
            }
         }
         logger.info("Sent {}", nMessages);
         session.commit();

         MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION), "originalI >= 100 AND originalI < 90000");
         connection.start();
         for (int i = 100; i < nMessages - 1000; i++) {
            Message message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("originalI"));
            if (i % 1000 == 0) {
               logger.info("Receiving {}", i);
               session.commit();
            }
         }
         session.commit();
         logger.info("Received {}", nMessages - 1);


         for (int i = nMessages; i < nMessages + 5000; i++) {
            Message message = session.createTextMessage(BODY_STRING);
            message.setIntProperty("originalI", i);
            producer.send(message);
            if (i % 1000 == 0) {
               session.commit();
               logger.info("Sending {}", i);
            }
         }
         session.commit();
      }

      destroyLive();

      File pagingFolder = new File(getServerLocation(SERVER_NAME_0), "/data/paging");
      FileUtil.deleteDirectory(pagingFolder);
      pagingFolder.mkdirs();

      startLive();

      long messageCount;

      try (SimpleManagement simpleManagement = new SimpleManagement("tcp://localhost:61616", "guest", "guest")) {
         messageCount = simpleManagement.getMessageCountOnQueue(OSCILLATE_QUEUE_NAME);
         logger.info("There still {}", messageCount);
      }

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer messageConsumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();
         for (int i = 0; i < messageCount; i++) {
            Message message = messageConsumer.receive(5000);
         }
      }

      int lastMessages = 10_000;

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));

         for (int i = 0; i < lastMessages; i++) {
            TextMessage message = session.createTextMessage(BODY_STRING);
            message.setIntProperty("i", i);
            producer.send(message);
            if (i % 1000 == 0) {
               session.commit();
               logger.info("last Sending {}", i);
            }
         }
         logger.info("last Sent {}", lastMessages);
         session.commit();
      }

      destroyLive();

      startLive();

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer fastConsumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();
         for (int i = 0; i < lastMessages; i++) {
            Message message = fastConsumer.receive(5000);
            if (message == null) {
               logger.info("Expected message at {}", i);
            }
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("i"));
            if (i % 1000 == 0) {
               logger.info("final Receiving {}", i);
               session.commit();
            }
         }
         session.commit();
         logger.info("final Received {}", nMessages - 1);
      }

   }

   @Test
   public void testOscilateWhileFailingBack() throws Exception {
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      runAfter(executorService::shutdownNow);

      configureAddress0(null);
      configureAddress1(null);
      startLive();
      startBackup();
      waitReplicaSync();

      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      ActiveMQConnectionFactory slowConsumerFactory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      slowConsumerFactory.setConsumerWindowSize(0);

      int nMessages = 500;

      for (int repeat = 0; repeat < 20; repeat++) {
         logger.info("Repeat ######################################################################################################################## {}", repeat);
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            if (repeat == 0) {
               MessageProducer producer = session.createProducer(session.createQueue(BUSY_QUEUE));
               for (int i = 0; i < nMessages * 2; i++) {
                  Message message = session.createTextMessage(BODY_STRING);
                  message.setIntProperty(propertyIdentification, i);
                  message.setStringProperty("_AMQ_GROUP_ID", "group" + (i % 10));
                  producer.send(message);
                  if (i % 1000 == 0) {
                     session.commit();
                     logger.info("Sending {}", i);
                  }
               }
               session.commit();
               producer.close();
            }

            MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));
            for (int i = 0; i < nMessages; i++) {
               Message message = session.createTextMessage(BODY_STRING);
               message.setIntProperty(propertyIdentification, i);
               message.setStringProperty("_AMQ_GROUP_ID", "group" + (i % 10));
               producer.send(message);
               if (i % 1000 == 0) {
                  session.commit();
                  logger.info("Sending {}", i);
               }
            }
            logger.info("Sent {}", nMessages);
            session.commit();

            MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
            connection.start();
            for (int i = 0; i < nMessages; i++) {
               Message message = consumer.receive(5000);
               assertNotNull(message);
               assertEquals(i, message.getIntProperty(propertyIdentification));
               if (i % 1000 == 0) {
                  logger.info("Receiving {}", i);
                  session.commit();
               }
            }
            session.commit();
            logger.info("Received {}", nMessages - 1);
            consumer.close();
         }

         CountDownLatch done = new CountDownLatch(2);
         AtomicBoolean running = new AtomicBoolean(true);
         runAfter(() -> running.set(false));
         executorService.execute((() -> loadGeneratorConsumer(running, done)));
         executorService.execute((() -> loadGeneratorProducer(running, done)));

         long firstPage = getFirstPageID();

         logger.info("First page before starting {}", firstPage);

         Thread.sleep(5000);
         // rebooting backup
         destroyBackup();
         startBackup();

         //rebooting backup with a delay to restart it
         logger.info("******************************************************************************************************************************* Destroying live");
         destroyLive();
         Thread.sleep(5000); // allow some processing on backup
         logger.info("******************************************************************************************************************************* starting live");
         startLive();
         waitReplicaSync();
         // restart just live now
         destroyLive();
         startLive();
         waitReplicaSync();

         running.set(false);
         assertTrue(done.await(1, TimeUnit.MINUTES));

         waitReplicaSync();

         long firstPageAtEnd = getFirstPageID();

         logger.info("First page at end = {}", firstPageAtEnd);

         if (firstPageAtEnd < firstPage) {
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> XXX First page was {}, and now first page is {}", firstPage, firstPageAtEnd);
         }

         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
            connection.start();
            while (true) {
               Message message = consumer.receive(500);
               if (message == null) {
                  break;
               }
            }
         }

      }
   }

   private static long getFirstPageID() throws Exception {
      ScheduledExecutorService scheduledExecutor = null;
      ExecutorService executor = null;

      HierarchicalObjectRepository<AddressSettings> settings = new HierarchicalObjectRepository<>();
      settings.addMatch("#", new AddressSettings());

      PagingStoreFactory pagingStoreFactory = new PagingStoreFactoryNIO(new NullStorageManager(), new File(getServerLocation(SERVER_NAME_0), "/data/paging"), 1000, scheduledExecutor, new OrderedExecutorFactory(executor), new OrderedExecutorFactory(executor), true, null);
      PagingManagerImpl pagingManager = new PagingManagerImpl(pagingStoreFactory, settings);
      pagingManager.start();
      PagingStore store = pagingManager.getPageStore(SimpleString.toSimpleString(OSCILLATE_ADDRESS));
      long firstPage = store.getFirstPage();
      store.stop();
      pagingManager.stop();
      logger.info("current first page {}", firstPage);

      return firstPage;
   }

   public void loadGeneratorConsumer(AtomicBoolean running, CountDownLatch done) {

      try {
         while (running.get()) {

            try (Connection connection = connectEither(running)) {
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
               connection.start();
               long consumed = 0;
               while (running.get()) {
                  Message message = consumer.receive(5000);
                  if (message != null) {
                     consumed++;
                     if (consumed % 100 == 0) {
                        logger.info("Load on consumer {}", consumed);
                     }
                  }
               }

            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            }

         }
      } finally {
         done.countDown();
      }
   }


   public void loadGeneratorProducer(AtomicBoolean running, CountDownLatch done) {

      try {
         while (running.get()) {

            try (Connection connection = connectEither(running)) {
               Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
               MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));
               long sent = 0;
               while (running.get()) {
                  Message message = session.createTextMessage(BODY_STRING);
                  message.setStringProperty("loadGenerator", "yes");
                  producer.send(message);
                  sent++;
                  if (sent % 25 == 0) {
                     logger.info("load on producer {}", sent);
                     session.commit();
                     Thread.sleep(100);
                  }
               }
               session.commit();

            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            }

         }
      } finally {
         done.countDown();
      }
   }

   public Connection connectEither(AtomicBoolean running) {
      while (running.get()) {
         try {
            ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
            return factory.createConnection();
         } catch (Exception ignored) {
         }
         try {
            ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61617");
            return factory.createConnection();
         } catch (Exception ignored) {
         }
      }
      return null;
   }

   void messageAdded(Integer messageID) {
      receivedMessages.put(messageID, messageID);
      produced.incrementAndGet();
   }

   void messageFound(Integer messageID) {
      receivedMessages.remove(messageID);
      consumed.incrementAndGet();
   }

   private void oscillateTXConsumer() {
      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      factory.setConsumerWindowSize(0);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();
         boolean commit = true;
         HashSet<Integer> localReceived = new HashSet<>();
         while (running) {
            localReceived.clear();
            for (int i = 0; i < 10; i++) {
               Message message = consumer.receive(1000);
               if (message != null) {
                  localReceived.add(message.getIntProperty(propertyIdentification));
               }
            }

            if (commit) {
               commit = false;
               session.commit();
               localReceived.forEach(this::messageFound);
            } else {
               commit = true;
               session.rollback();
            }
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }

   }

   private void oscillateProducer() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));

         long waitOscillation = 500;

         while (running) {
            for (int i = 0; i < 100; i++) {
               TextMessage message = session.createTextMessage(BODY_STRING);
               int id = sequenceGenerator.incrementAndGet();
               message.setIntProperty(propertyIdentification, id);
               messageAdded(id);
               producer.send(message);
               session.commit();
            }
            logger.info("Sent on oscillation = {}, currentSleep={}", produced.get(), waitOscillation);
            Thread.sleep(waitOscillation);
            waitOscillation -= 100;
            if (waitOscillation <= 100) {
               waitOscillation = 1000;
            }

            try {
               barrierOscillation.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
               Thread.currentThread().interrupt();
            } catch (Exception dontCare) {
               logger.debug(dontCare.getMessage(), dontCare);
            }

         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }

   }

   private void oscillateSpikeProducer() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(OSCILLATE_ADDRESS));

         long SPIKE_INTERVAL = 20_000;
         int SPIKE_SIZE = 10_0000;

         long nextSpike = System.currentTimeMillis() + SPIKE_INTERVAL;

         while (running) {
            while (nextSpike > System.currentTimeMillis() && running) {
               Thread.sleep(1000);
               logger.info("Generating spike in {} milliseconds", nextSpike - System.currentTimeMillis());
            }

            if (!running) {
               break;
            }

            logger.info("Generating a spike");

            int sent = 0;
            for (int i = 0; i < SPIKE_SIZE; i++) {
               TextMessage message = session.createTextMessage(BODY_STRING);
               int id = sequenceGenerator.incrementAndGet();
               message.setIntProperty(propertyIdentification, id);
               messageAdded(id);
               producer.send(message);
               sent++;
               if (i % 1000 == 0) {
                  logger.info("Spike sent {}", i);
                  session.commit();
                  sent = 0;
               }
            }
            session.commit();
            logger.info("Generated spike of {}", SPIKE_SIZE);
            nextSpike = System.currentTimeMillis() + SPIKE_INTERVAL;
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }

   }


   private void finalConsume() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         SimpleManagement simpleManagement = new SimpleManagement("tcp://localhost:61616", "guest", "guest");

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();

         for(;;) {
            Message message = consumer.receive(500);
            if (message == null) {
               break;
            }
            messageFound(message.getIntProperty(propertyIdentification));
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }

   private void restartReplica() {
      try {
         Thread.sleep(20_000);
         destroyBackup();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }

   private void oscillateConsumer() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();

         long rec = 0;

         while (running) {
            TextMessage message = (TextMessage) consumer.receive(100);
            if (message != null) {
              if (++rec % 200 == 0) {
                 logger.info("Received {} on oscillateConsumer", rec);
              }
              messageFound(message.getIntProperty(propertyIdentification));
            }
         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }


   private void oscillateBrowser() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         while (running) {
            try (QueueBrowser browser = session.createBrowser(session.createQueue(OSCILLATE_QUEUE_EXPRESSION))) {
               Enumeration<Message> messageEnum = browser.getEnumeration();
               int messagesOnBrowser = 0;
               while (running && messageEnum.hasMoreElements()) {
                  Message message = messageEnum.nextElement();
                  if (message != null) {
                     messagesOnBrowser++;
                  }
               }
               logger.info("there was {} messages through browsing", messagesOnBrowser);
               Thread.sleep(250);
            }
            connection.start();
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }


   private void oscillateSlowConsumer() {
      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      factory.setConsumerWindowSize(0);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue(OSCILLATE_QUEUE_EXPRESSION));
         connection.start();

         HashSet<Integer> localReceived = new HashSet<>();

         while (running) {
            Thread.sleep(100);
            localReceived.clear();
            for (int i = 0; i < 2; i++) {
               TextMessage message = (TextMessage) consumer.receive(100);
               if (message != null) {
                  localReceived.add(message.getIntProperty(propertyIdentification));
                  logger.info("Slow Received {}", message.getIntProperty(propertyIdentification));
               }
               Thread.sleep(200);
            }
            Thread.sleep(1000);
            session.commit();
            logger.info("Commit slow");
            localReceived.forEach(this::messageFound);
         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }



   private void busyProducer() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(BUSY_QUEUE));

         while (running) {
            producer.send(session.createTextMessage(BODY_STRING));
            session.commit();
         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }

   }

   private void busyConsumer() {
      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(BUSY_QUEUE));
         connection.start();

         long rec = 0;

         while (running) {
            TextMessage message = (TextMessage) consumer.receive(100);
            if (message != null && rec++ % 1000 == 0) {
               logger.debug("Received {} on busy", rec);
            }
         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         errors.incrementAndGet();
      } finally {
         done.countDown();
      }
   }
}

