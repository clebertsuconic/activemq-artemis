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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.jdbc.parallelDB.BatchableStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStatement extends BatchableStatement<StatementsManager.MessageTask> {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public MessageStatement(Connection connection, JDBCConnectionProvider connectionProvider, String tableName, int expectedSize) throws SQLException {
      super(connectionProvider, connection, connectionProvider.getSQLProvider().getInsertPDBMessages(tableName), expectedSize);
   }

   @Override
   protected void doOne(StatementsManager.MessageTask task) throws Exception {
      ActiveMQBuffer buffer = getPersistedBuffer(task.message.getPersister(), task.message);
      preparedStatement.setLong(1, task.message.getMessageID());
      preparedStatement.setBlob(2, blobInputStream(buffer));
      if (task.tx != null) {
         preparedStatement.setLong(3, task.tx);
      } else {
         preparedStatement.setNull(3, Types.NUMERIC);
      }
   }

}
