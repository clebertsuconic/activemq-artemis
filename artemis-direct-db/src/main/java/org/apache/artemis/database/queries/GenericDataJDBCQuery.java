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

package org.apache.artemis.database.queries;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.data.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericDataJDBCQuery {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Connection connection;
   DatabaseProvider databaseProvider;
   String tableName;

   public GenericDataJDBCQuery(DatabaseProvider databaseProvider, Connection connection) {
      this(databaseProvider, connection, databaseProvider.getSqlProvider().getBrokerData());
   }

   public GenericDataJDBCQuery(DatabaseProvider databaseProvider, Connection connection, String tableName) {
      this.databaseProvider = databaseProvider;
      this.connection = connection;
      this.tableName = tableName;
   }

   public void query(Consumer<GenericData> consumer) throws Exception {
      String sql = databaseProvider.getSqlProvider().selectGenericData(tableName);
      Statement statement = connection.createStatement();
      statement.setFetchSize(500);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
         while (resultSet.next()) {
            long id = resultSet.getLong(1);
            byte recordType = (byte) resultSet.getShort(2);
            long txIdValue = resultSet.getLong(3);
            Long txId = resultSet.wasNull() ? null : txIdValue;

            byte[] bytes = resultSet.getBytes(4);
            ActiveMQBuffer buffer = null;
            if (bytes != null) {
               buffer = ActiveMQBuffers.wrappedBuffer(bytes);
            }

            final ActiveMQBuffer finalBuffer = buffer;
            GenericData data = new GenericData(id, recordType, txId, finalBuffer != null ? () -> finalBuffer : null);
            consumer.accept(data);
         }
      }
   }
}
