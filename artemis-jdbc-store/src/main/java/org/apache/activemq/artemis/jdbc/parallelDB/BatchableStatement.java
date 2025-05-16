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

package org.apache.activemq.artemis.jdbc.parallelDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.io.IOCallback;

public abstract class BatchableStatement<E> {

   Connection connection;
   protected PreparedStatement preparedStatement;

   List<E> pendingList;
   List<IOCallback> callbacks;

   public void BatchableStatement(Connection connection, String statement, int expectedSize) throws Exception {
      this.connection = connection;
      this.connection.prepareStatement(statement);
      this.pendingList = new ArrayList<>(expectedSize);
      this.callbacks = new ArrayList<>(expectedSize);
   }

   public void addData(E element, IOCallback callback) {
      pendingList.add(element);
      callbacks.add(callback);
   }

   public void flushPending() throws Exception {
      pendingList.forEach(this::flushOne);
   }

   protected void flushOne(E element) {
      try {
         doOne(element);
         preparedStatement.addBatch();
      } catch (SQLException e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   protected abstract void doOne(E element);

}
