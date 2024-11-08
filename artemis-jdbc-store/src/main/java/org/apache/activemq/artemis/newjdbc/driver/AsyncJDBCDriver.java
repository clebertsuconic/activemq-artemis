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

package org.apache.activemq.artemis.newjdbc.driver;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncJDBCDriver implements AutoCloseable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final Connection jdbConnection;

   final String sqlStatement;

   final PreparedStatement preparedStatement;

   public AsyncJDBCDriver(Connection jdbConnection, String sqlStatement) throws Exception {
      this.jdbConnection = jdbConnection;
      this.sqlStatement = sqlStatement;
      this.preparedStatement = jdbConnection.prepareStatement(sqlStatement);
   }

   public void close() throws Exception {
      if (preparedStatement != null) {
         preparedStatement.close();
      }
   }

   public void execute(List<Consumer<PreparedStatement>> setters, List<Runnable> completion) throws Exception {
      try {
         setters.forEach(this::prepareBatch);
         preparedStatement.executeBatch();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         throw new RuntimeException(e.getMessage(), e);
      }
      jdbConnection.commit();
      completion.forEach(Runnable::run);
   }

   public void executeOneByOne(List<Consumer<PreparedStatement>> setters, List<Runnable> completion) throws Exception {
      try {
         int counter = 0;
         for (Consumer<PreparedStatement> setter : setters) {
            setter.accept(preparedStatement);
            preparedStatement.execute();
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         throw new RuntimeException(e.getMessage(), e);
      }
      jdbConnection.commit();
      completion.forEach(Runnable::run);
   }

   private void prepareBatch(Consumer<PreparedStatement> setter) {
      try {
         setter.accept(preparedStatement);
         preparedStatement.addBatch();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         throw new RuntimeException(e.getMessage(), e);
      }
   }





}
