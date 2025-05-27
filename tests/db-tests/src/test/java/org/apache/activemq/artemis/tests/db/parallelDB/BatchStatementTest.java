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

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.MessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.ReferencesStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.StatementsManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class BatchStatementTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   DatabaseStorageConfiguration storageConfiguration;
   Configuration configuration;

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
      this.configuration.setStoreConfiguration(storageConfiguration);

      executorService = Executors.newFixedThreadPool(5);
      runAfter(executorService::shutdownNow);

      scheduledExecutorService = Executors.newScheduledThreadPool(5);
      runAfter(scheduledExecutorService::shutdownNow);

      executorFactory = new OrderedExecutorFactory(executorService);
      criticalAnalyzer = Mockito.mock(CriticalAnalyzer.class);
   }


   @TestTemplate
   public void testReferencesDirectly() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      IOCallback ioCallback = new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         ReferencesStatement referencesStatement = new ReferencesStatement(connection, connectionProvider, storageConfiguration.getParallelDBReferences(), 100);
         for (int i = 1; i <= nrecords; i++) {
            StatementsManager.MessageReferenceTask task = parallelDBStorageManager.getStatementsManager().newReferenceTask(i, 1, i % 2 == 0 ? (long)i : null, ioCallback);
            referencesStatement.addData(task, ioCallback);
         }
         referencesStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "ART_REFERENCES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   private static MessageReference mockReference(long messageID) {
      MessageReference reference = Mockito.mock(MessageReference.class);
      Mockito.doAnswer((a) -> messageID).when(reference).getMessageID();
      Mockito.doAnswer(a -> 1L).when(reference).getQueueID();
      return reference;
   }

   @TestTemplate
   public void testMessagesDirectly() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      IOCallback ioCallback = new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         MessageStatement messageStatement = new MessageStatement(connection, connectionProvider, storageConfiguration.getParallelDBMessages(), 100);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            StatementsManager.MessageTask task = parallelDBStorageManager.getStatementsManager().newMessageTask(message, null, ioCallback);
            messageStatement.addData(task, ioCallback);
         }
         messageStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "ART_MESSAGES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testMessagesStorageManager() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int nrecords = 100;

      CountDownLatch latchDone = new CountDownLatch(1);

      StatementsManager statementsManager = parallelDBStorageManager.getStatementsManager();

      OperationContext context = parallelDBStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      for (int i = 1; i <= nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
         message.setMessageID(i);
         message.getBodyBuffer().writeByte((byte) 'Z');
         statementsManager.storeMessage(message, null, OperationContextImpl.getContext());
         statementsManager.flushTL();
      }

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latchDone.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBMessages()));
   }


   @TestTemplate
   public void testMessagesReferencesStorageManager() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int pairs = 5_000;
      int nrecords = pairs * 2;

      CountDownLatch latchDone = new CountDownLatch(1);

      StatementsManager statementsManager = parallelDBStorageManager.getStatementsManager();

      OperationContext context = parallelDBStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      for (int i = 1; i <= nrecords; i++) {
         if (i % 2 == 0) {
            parallelDBStorageManager.storeReference(1, i, true);
         } else {
            parallelDBStorageManager.storeReferenceTransactional(i, 1, i);
         }
         statementsManager.flushTL();
      }

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latchDone.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBReferences()));
      assertEquals(pairs, selectNumber(connection, "SELECT COUNT(*) FROM " + storageConfiguration.getParallelDBReferences() + " WHERE TX_ID IS NULL"));
      assertEquals(pairs, selectNumber(connection, "SELECT COUNT(*) FROM " + storageConfiguration.getParallelDBReferences() + " WHERE TX_ID IS NOT NULL"));
   }



   @TestTemplate
   public void testTreatExceptionOnError() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();


      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      AtomicInteger errors = new AtomicInteger(0);

      IOCallback ioCallback = new IOCallback() {
         @Override
         public void done() {
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
            errors.incrementAndGet();
         }
      };

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         MessageStatement messageStatement = new MessageStatement(connection, connectionProvider, storageConfiguration.getParallelDBMessages(), 100);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(1); // everything should fail with a DuplicateException
            message.getBodyBuffer().writeByte((byte) 'Z');

            messageStatement.addData(parallelDBStorageManager.getStatementsManager().newMessageTask(message, null, ioCallback), ioCallback);
         }
         assertThrows(SQLException.class, () -> messageStatement.flushPending(true));

         // forcing a commit, even though it failed... it should not commit any success
         connection.commit();

         assertEquals(0, selectCount(connection, "ART_MESSAGES"));
      }

      assertEquals(nrecords, errors.get());

   }

}