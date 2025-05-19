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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.jdbc.parallelDB.BatchableStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;

public class ReferencesTXStatement extends BatchableStatement<ReferencesTXStatement.Data> {

   public ReferencesTXStatement(Connection connection, JDBCConnectionProvider connectionProvider, String tableName, int expectedSize) throws SQLException {
      super(connectionProvider, connection, connectionProvider.getSQLProvider().getInsertPDBReferencesTX(tableName), expectedSize);
   }

   public static class Data {
      long messageID;
      long queueID;
      long tx;

      public Data(long messageID, long queueID, long tx) {
         this.messageID = messageID;
         this.queueID = queueID;
         this.tx = tx;
      }
   }

   @Override
   protected void doOne(Data reference) throws Exception {
      preparedStatement.setLong(1, reference.messageID);
      preparedStatement.setLong(2, reference.queueID);
      preparedStatement.setLong(3, reference.tx);
   }

}
