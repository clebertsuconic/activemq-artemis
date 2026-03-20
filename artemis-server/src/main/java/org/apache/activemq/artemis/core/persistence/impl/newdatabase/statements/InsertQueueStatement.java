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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.QueueData;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertQueueStatement extends BatchableStatement<QueueData> {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public InsertQueueStatement(Connection connection, JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseStorageConfiguration, int expectedSize) throws SQLException {
      super(connectionProvider, connection, getSQL(connectionProvider, databaseStorageConfiguration), expectedSize);
   }

   private static String getSQL(JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseStorageConfiguration) {
      // TODO parameterize this
      return "INSERT INTO QUEUE_INFO (QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST, IS_ANYCAST, FILTER_STRING) VALUES (?, ?, ?, ?, ?, ?)";
   }

   @Override
   protected void doOne(QueueData task) throws Exception {
      preparedStatement.setLong(1, task.id);
      preparedStatement.setLong(2, task.addressId);
      preparedStatement.setString(3, task.name);
      preparedStatement.setString(4, task.isMulticast ? "Y" : "N");
      preparedStatement.setString(5, task.isAnycast ? "Y" : "N");
      // 1. Convert the String to a byte array using the desired character encoding
      if (task.filter != null) {
         byte[] stringBytes = task.filter.getBytes(StandardCharsets.UTF_8);
         InputStream blobStream = new ByteArrayInputStream(stringBytes);
         preparedStatement.setBlob(6, blobStream);
      } else {
         preparedStatement.setNull(6, Types.BLOB);
      }
   }

}
