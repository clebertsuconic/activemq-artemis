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

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.MessageData;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagesJDBCQuery {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Connection connection;
   public MessagesJDBCQuery(Connection connection) {
      this.connection = connection;
   }

   public void query(Consumer<MessageData> consumer) throws Exception {
      Statement statement = connection.createStatement();
      statement.setFetchSize(500);
      try (ResultSet resultSet = statement.executeQuery("SELECT MESSAGE_ID, MESSAGE_RECORD FROM ART_MESSAGES ORDER BY MESSAGE_ID")) {
         while (resultSet.next()) {
            Blob blob = resultSet.getBlob(2);
            int bodySize = (int)blob.length();
            byte[] bytes = new byte[bodySize];
            try (InputStream inputStream = blob.getBinaryStream()) {
               inputStream.read(bytes);
            }

            ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(bytes);

            Message message = MessagePersister.getInstance().decode(buffer, null, null);
            message.setMessageID(resultSet.getLong(1));

            MessageData messageData = new MessageData(message, null);
            consumer.accept(messageData);
         }
      }
   }



}
