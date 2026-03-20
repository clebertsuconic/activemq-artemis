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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.QueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueJDBCQuery {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Connection connection;
   public QueueJDBCQuery(Connection connection) {
      this.connection = connection;
   }

   public void query(Consumer<QueueData> consumer) throws Exception {
      Statement statement = connection.createStatement();
      statement.setFetchSize(500);
      try (ResultSet resultSet = statement.executeQuery("SELECT QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST, IS_ANYCAST, FILTER_STRING FROM QUEUE_INFO ORDER BY QUEUE_ID")) {
         while (resultSet.next()) {
            long queueID = resultSet.getLong(1);
            long addressID = resultSet.getLong(2);
            String queueName = resultSet.getString(3);
            boolean isMulticast = String.valueOf(resultSet.getString(4)).equals("Y");
            boolean isAnycast = String.valueOf(resultSet.getString(5)).equals("Y");

            String filter = null;
            Blob blob = resultSet.getBlob(6);
            if (blob != null) {
               int filterSize = (int)blob.length();
               byte[] filterBytes = blob.getBytes(1, filterSize);
               filter = new String(filterBytes, StandardCharsets.UTF_8);
            }
            QueueData data = new QueueData(addressID, queueID, queueName, filter, isMulticast, isAnycast, null);
            consumer.accept(data);
         }
      }
   }



}
