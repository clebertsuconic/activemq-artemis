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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.MessageReferenceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReferencesJDBCQuery {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Connection connection;
   public ReferencesJDBCQuery(Connection connection) {
      this.connection = connection;
   }

   public void query(Consumer<MessageReferenceData> consumer) throws Exception {
      Statement statement = connection.createStatement();
      statement.setFetchSize(500);
      try (ResultSet resultSet = statement.executeQuery("SELECT MESSAGE_ID, QUEUE_ID FROM ART_REFERENCES ORDER BY MESSAGE_ID, QUEUE_ID")) {
         while (resultSet.next()) {
            long messageID = resultSet.getLong(1);
            long queueID = resultSet.getLong(2);
            MessageReferenceData referenceData = new MessageReferenceData(messageID, queueID, null);
            consumer.accept(referenceData);
         }
      }
   }



}
