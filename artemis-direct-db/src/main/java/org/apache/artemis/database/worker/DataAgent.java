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

package org.apache.artemis.database.worker;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.artemis.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataAgent will provide connection, reconnection, running and retries for DataWorker */
public abstract class DataAgent implements Runnable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final DatabaseProvider databaseProvider;
   protected Connection connection;

   protected DataAgent(DatabaseProvider databaseProvider) throws SQLException {
      this.databaseProvider = databaseProvider;
   }

   protected void connect() throws SQLException {
      connection = databaseProvider.getConnection();
      connection.setAutoCommit(false);
   }

   @Override
   public void run() {
      int success = 0;
      SQLException lastException = null;
      try {
         for (int retryI = 0; retryI < 5 && success == 0; retryI++) {
            try {
               doBeforeCommit();
               // notice: adding the commit here, would require us to validate each individual date before we retry.
               // this is because the commit could fail in the way back from the database
               success++;
            } catch (SQLException e) {
               lastException = e;
               logger.warn("Retrying Connection:: {}", e.getMessage(), e);
               try {
                  connection.rollback();
                  connection.close();
               } catch (Throwable ignored) {
               }

               try {
                  connect();
               } catch (SQLException connectingException) {
                  // TODO: criticalError
                  logger.warn(e.getMessage(), e);
               }
            }
         }
         connection.commit();
         if (success > 0) {
            doAfterCommit();
         } else {
            doError(lastException);
         }
      } catch (Exception e) {
         // TODO Critical IO Error...
         logger.warn(e.getMessage(), e);
      } finally {
         doCleanup();
      }
   }

   protected abstract void doAfterCommit();
   protected abstract void doError(Exception exception);
   protected abstract void doBeforeCommit() throws SQLException;
   protected abstract void doCleanup();
}
