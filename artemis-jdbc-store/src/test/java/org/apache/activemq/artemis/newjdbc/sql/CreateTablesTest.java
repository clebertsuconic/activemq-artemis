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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.function.Consumer;

import org.apache.activemq.artemis.newjdbc.driver.AsyncJDBCDriver;
import org.apache.activemq.artemis.newjdbc.driver.SyncJDBCDriver;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.apache.activemq.artemis.tests.util.DBSupportUtil;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class CreateTablesTest extends ArtemisTestCase {

   static final String DERBY_DB = "memory:test";

   @AfterEach
   public void shutdownDerby() throws Exception{
      DBSupportUtil.dropDerbyDatabase(null, null, DERBY_DB);
      DBSupportUtil.shutdownDerby(null, null);
   }

   @Test
   public void testCreate()  throws Exception {
      try (Connection conn = DriverManager.getConnection("jdbc:derby:" + DERBY_DB + ";create=true")) {
         PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(PropertySQLProvider.Factory.SQLDialect.GENERIC);
         SQLProvider provider = factory.create();
         provider.createTables(conn);
      }
   }


   @Test
   public void testInsertMessages()  throws Exception {
      try (Connection conn = DriverManager.getConnection("jdbc:derby:" + DERBY_DB + ";create=true")) {
         PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(PropertySQLProvider.Factory.SQLDialect.GENERIC);
         SQLProvider provider = factory.create();
         provider.createTables(conn);

         AsyncJDBCDriver asyncJDBCDriver = provider.createStoreMessagesDriver(conn);

         ArrayList<Consumer<PreparedStatement >> list = new ArrayList<>();
         ArrayList<Runnable> doneList = new ArrayList<>();
         for (int i = 0; i < 10_000; i++) {
            list.add(this::setMessage);
            doneList.add(this::done);
         }

         asyncJDBCDriver.execute(list, doneList);

      }
   }

   @Test
   public void testInsertMessagesOneByOne()  throws Exception {
      try (Connection conn = DriverManager.getConnection("jdbc:derby:" + DERBY_DB + ";create=true")) {
         PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(PropertySQLProvider.Factory.SQLDialect.GENERIC);
         SQLProvider provider = factory.create();
         provider.createTables(conn);

         AsyncJDBCDriver asyncJDBCDriver = provider.createStoreMessagesDriver(conn);

         ArrayList<Consumer<PreparedStatement >> list = new ArrayList<>();
         ArrayList<Runnable> doneList = new ArrayList<>();
         for (int i = 0; i < 10_000; i++) {
            list.add(this::setMessage);
            doneList.add(this::done);
         }

         asyncJDBCDriver.executeOneByOne(list, doneList);

      }
   }

   int done;

   public void done() {
      System.out.println("Done " + (done++));
   }

   int counter;
   public void setMessage(PreparedStatement set) {
      try {
         set.setLong(1, counter++);
         set.setBytes(2, new byte[300]);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }

   }


}
