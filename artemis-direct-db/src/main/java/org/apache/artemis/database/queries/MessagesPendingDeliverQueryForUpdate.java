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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.artemis.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagesPendingDeliverQueryForUpdate {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Connection connection;
   DatabaseProvider databaseProvider;
   PreparedStatement deliveryPreparedStatement;
   PreparedStatement updateDeliveryStatement;

   public MessagesPendingDeliverQueryForUpdate(DatabaseProvider databaseProvider, Connection connection) {
      this.databaseProvider = databaseProvider;
      this.connection = connection;
   }

   public void prepare() throws SQLException {
      String messagesTable = databaseProvider.getSqlProvider().getMessages();
      String referencesTable = databaseProvider.getSqlProvider().getRefs();
      String deliverSQL = databaseProvider.getSqlProvider().deliverPendingMessages(messagesTable, referencesTable);
      deliveryPreparedStatement = connection.prepareStatement(deliverSQL);

      String updateSql = databaseProvider.getSqlProvider().updatePendingDelivery(referencesTable);
      updateDeliveryStatement = connection.prepareStatement(updateSql);
   }

   public void updateDelivery(long queueID, long messageID) throws Exception {
      updateDeliveryStatement.setLong(1, queueID);
      updateDeliveryStatement.setLong(2, messageID);
      updateDeliveryStatement.addBatch();
   }

   public void flush() throws Exception {
      updateDeliveryStatement.executeBatch();
   }


   public void close() throws Exception {
      deliveryPreparedStatement.close();
      updateDeliveryStatement.close();
   }

   public ResultSet execute(long queueID) throws Exception {
      deliveryPreparedStatement.setLong(1, queueID);
      return deliveryPreparedStatement.executeQuery();
   }


}
