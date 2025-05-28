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

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class TXStatementsTest extends AbstractStatementTest {

   @TestTemplate
   public void testTXMessagesStorageManager() throws Exception {
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

      assertTrue(nrecords % 2 == 0, "nrecords must be even");

      CountDownLatch latchDone = new CountDownLatch(1);

      StatementsManager statementsManager = parallelDBStorageManager.getStatementsManager();

      OperationContext context = parallelDBStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      long txID = 10;
      for (int i = 1; i <= nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
         message.setMessageID(i);
         message.getBodyBuffer().writeByte((byte) 'Z');
         statementsManager.storeMessage(message, txID, OperationContextImpl.getContext());
         statementsManager.storeReference(i, 1, txID, context);
         statementsManager.flushTL();
         if (i == nrecords / 2) {
            txID = 11;
         }
      }

      statementsManager.storeTX(10, true, true, context);
      statementsManager.flushTL();

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
      assertEquals(nrecords / 2, selectNumber(connection, "SELECT COUNT(*) FROM ART_MESSAGES WHERE TX_ID=10 AND TX_VALID IS NOT NULL"));
      assertEquals(nrecords / 2, selectNumber(connection, "SELECT COUNT(*) FROM ART_REFERENCES WHERE TX_ID=10 AND TX_VALID IS NOT NULL"));
      // we did not update 11
      assertEquals(nrecords / 2, selectNumber(connection, "SELECT COUNT(*) FROM ART_MESSAGES WHERE TX_ID=11 AND TX_VALID IS NULL"));
      assertEquals(nrecords / 2, selectNumber(connection, "SELECT COUNT(*) FROM ART_REFERENCES WHERE TX_ID=11 AND TX_VALID IS NULL"));
   }
}