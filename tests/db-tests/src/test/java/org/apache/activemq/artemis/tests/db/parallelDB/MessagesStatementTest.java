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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.MessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.ReferencesStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.StatementsManager;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class MessagesStatementTest extends AbstractStatementTest {

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