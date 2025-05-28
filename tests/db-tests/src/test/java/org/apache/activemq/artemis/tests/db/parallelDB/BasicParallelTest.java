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
package org.apache.activemq.artemis.tests.db.parallelDB;

import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.MessageStatement;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class BasicParallelTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Configuration configuration;
   DatabaseStorageConfiguration storageConfiguration;

   ExecutorService executorService;

   ScheduledExecutorService scheduledExecutorService;

   ExecutorFactory executorFactory;

   CriticalAnalyzer criticalAnalyzer;

   @Parameters(name = "db={0}")
   public static Collection<Object[]> parameters() {
      List<Database> dbList = Database.selectedList();
      dbList.remove(Database.DERBY); // no derby on this test

      return convertParameters(dbList);
   }

   // Used in @DisabledIf on class, avoids no-params failure with only -PDB-derby-tests
   public static boolean isNoDatabaseSelected() {
      return parameters().isEmpty();
   }

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();
      assumeTrue(database != Database.DERBY);
      dropDatabase();
   }

   @Override
   protected final String getJDBCClassName() {
      return database.getDriverClass();
   }

   @BeforeEach
   public void setupTest() throws Exception {
      storageConfiguration = createDefaultDatabaseStorageConfiguration();
      this.configuration = createDefaultNettyConfig();
      configuration.setStoreConfiguration(storageConfiguration);

      executorService = Executors.newFixedThreadPool(5);
      runAfter(executorService::shutdownNow);

      scheduledExecutorService = Executors.newScheduledThreadPool(5);
      runAfter(scheduledExecutorService::shutdownNow);

      executorFactory = new OrderedExecutorFactory(executorService);
      criticalAnalyzer = Mockito.mock(CriticalAnalyzer.class);
   }

   @TestTemplate
   public void testStoreMessage() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                   criticalAnalyzer,
                                   executorFactory,
                                   executorFactory,
                                   scheduledExecutorService);
      parallelDBStorageManager.start();

      CoreMessage message = new CoreMessage().initBuffer(10 * 1024).setDurable(true);

      message.setMessageID(333);
      message.getBodyBuffer().writeByte((byte)'Z');

      parallelDBStorageManager.storeMessage(message);
      parallelDBStorageManager.storeReference(1, 333, true);

      CountDownLatch done = new CountDownLatch(1);
      parallelDBStorageManager.getContext().executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            done.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      assertTrue(done.await(10, TimeUnit.SECONDS));
   }


   @TestTemplate
   public void testStoreMessageFromServer() throws Exception {

      ActiveMQServer server = createServer(true, configuration);

      server.start();

      int nMessages = 0;

      String[] protocols = {"CORE", "AMQP", "OPENWIRE"};
      for (String p : protocols) {
         ConnectionFactory factory = CFUtil.createConnectionFactory( p, "tcp://localhost:61616");
         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               producer.send(session.createTextMessage("test: " + p));
               session.commit();
               nMessages++;
               checkCounts(nMessages);
            }
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               producer.send(session.createTextMessage("test: " + p));
               nMessages++;
               checkCounts(nMessages);
            }
         }
      }
      checkCounts(nMessages);
      server.stop();

   }

   private void checkCounts(int messageCount) throws SQLException {
      try (Connection jdbcconnection = storageConfiguration.getConnectionProvider().getConnection()) {
         assertEquals(messageCount, selectCount(jdbcconnection, storageConfiguration.getParallelDBMessages()));
         assertEquals(messageCount, selectCount(jdbcconnection, storageConfiguration.getParallelDBReferences()));
      }
   }

   @TestTemplate
   public void testStoreMessageOnBatchableStatement() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessage(message);
         }
         latch.await(10, TimeUnit.SECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertEquals(100, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }


   @TestTemplate
   public void testStoreMessageOnBatchableStatementTX() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessageTransactional(3, message);
            parallelDBStorageManager.storeReferenceTransactional(3, 3, message.getMessageID());
            parallelDBStorageManager.commit(3);
         }
         latch.await(10, TimeUnit.SECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertEquals(100, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }

}