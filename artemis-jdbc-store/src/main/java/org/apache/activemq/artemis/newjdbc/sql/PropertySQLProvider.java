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

package org.apache.activemq.artemis.newjdbc.sql;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.Properties;

import org.apache.activemq.artemis.newjdbc.driver.SyncJDBCDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * Property-based implementation of a {@link org.apache.activemq.artemis.jdbc.store.sql.SQLProvider}'s factory.
 *
 * Properties are stored in a journal-sql.properties.
 *
 * Dialects specific to a database can be customized by suffixing the property keys with the name of the dialect.
 */
public class PropertySQLProvider implements SQLProvider {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Properties sqlProperties;
   protected PropertySQLProvider(Properties sqlProperties) {
      this.sqlProperties = sqlProperties;
   }

   @Override
   public void createTables(Connection connection) throws Exception {
      SyncJDBCDriver syncJDBCDriver = new SyncJDBCDriver(connection, getCreateMessagesTable());
      syncJDBCDriver.execute();
   }

   @Override
   public String getCreateMessagesTable() {
      return sqlProperties.getProperty("CREATE_MESSAGE_TABLE", "CREATE TABLE Messages (messageId BIGINT PRIMARY KEY, messageData BLOB)");
   }

   @Override
   public String getSelectMessagesTable() {
      return sqlProperties.getProperty("SELECT_MESSAGE_TABLE", "SELECT messageId, messageData from Messages");
   }

   @Override
   public String getInsertMessagesTable() {
      return sqlProperties.getProperty("INSERT_MESSAGE_TABLE", "INSERT INTO Messages (messageId, messageData) values (?, ?)");
   }

   public static final class Factory implements SQLProvider.Factory {

      private static final String SQL_PROPERTIES_FILE = "newjdbc-sql.properties";
      // can be null if no known dialect has been identified
      private SQLDialect dialect;

      public enum SQLDialect {
         GENERIC("generic", "generic");

         private final String key;
         private final String[] driverKeys;

         SQLDialect(String key, String... driverKeys) {
            this.key = key;
            this.driverKeys = driverKeys;
         }

         String getKey() {
            return key;
         }

         private boolean match(String driverName) {
            for (String driverKey : driverKeys) {
               if (driverName.contains(driverKey)) {
                  return true;
               }
            }
            return false;
         }

      }

      public SQLProvider create() {
         Properties properties = loadProperties(dialect.key + "-" + SQL_PROPERTIES_FILE);
         return new PropertySQLProvider(properties);
      }

      private static Properties loadProperties(String fileName) {
         Properties sqlProperties = new Properties();
         try (InputStream stream = PropertySQLProvider.class.getClassLoader().getResourceAsStream(fileName)) {
            sqlProperties.load(stream);
         } catch (Throwable e) {
            logger.warn("unable to find dialects file", e);
         }
         return sqlProperties;
      }

      public Factory(SQLDialect dialect) {
         this.dialect = dialect;
      }
   }

}
