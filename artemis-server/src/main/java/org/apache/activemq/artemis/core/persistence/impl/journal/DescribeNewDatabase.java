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

package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.io.PrintStream;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.utils.TableOut;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.queries.AddressJDBCQuery;
import org.apache.artemis.database.queries.GenericDataJDBCQuery;
import org.apache.artemis.database.queries.MessagesJDBCQuery;
import org.apache.artemis.database.queries.PageJDBCQuery;
import org.apache.artemis.database.queries.PageRefJDBCQuery;
import org.apache.artemis.database.queries.QueueJDBCQuery;
import org.apache.artemis.database.queries.ReferencesJDBCQuery;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;

public class DescribeNewDatabase {

   public static void describeDatabase(DatabaseStorageConfiguration dbConfig, PrintStream out, boolean safe) throws Exception {
      DatabaseProvider databaseProvider = dbConfig.getDatabaseProvider();
      try (Connection connection = databaseProvider.getConnection()) {


         printSection(out, "A D D R E S S E S", () -> printAddresses(databaseProvider, connection, out));

         printSection(out, "Q U E U E S", () -> printQueues(databaseProvider, connection, out));

         printSection(out, "C O N F I G   D A T A", () -> printGenericData(databaseProvider, connection, out, true));

         printSection(out, "B R O K E R   D A T A", () -> printGenericData(databaseProvider, connection, out, false));

         printSection(out, "M E S S A G E S", () -> printMessages(databaseProvider, connection, out, safe));

         printSection(out, "R E F E R E N C E S", () -> printReferences(databaseProvider, connection, out));

         printSection(out, "P A G E S", () -> printPages(databaseProvider, connection, out, safe));

         printSection(out, "P A G E   R E F E R E N C E S", () -> printPageRefs(databaseProvider, connection, out));
      }
   }

   private static void printSection(PrintStream out, String title, ThrowableRunnable runnable) {
      printBanner(out, title);
      try {
         runnable.run();
      } catch (Exception e) {
         out.println("WARNING: Could not read table: " + e.getMessage());
         e.printStackTrace(out);
         out.println();

      }
   }

   @FunctionalInterface
   private interface ThrowableRunnable {
      void run() throws Exception;
   }

   private static void printAddresses(DatabaseProvider databaseProvider, Connection connection, PrintStream out) throws Exception {
      int[] columnSizes = {10, 50, 10, 10};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"ID", "Name", "Multicast", "Anycast"});

      AtomicInteger count = new AtomicInteger();
      AddressJDBCQuery query = new AddressJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         tableOut.print(out, new String[]{String.valueOf(data.id), data.address, data.isMulticast ? "Y" : "N", data.isAnycast ? "Y" : "N"});
         count.incrementAndGet();
      });
      out.println("Total addresses: " + count.get());
      out.println();
   }

   private static void printQueues(DatabaseProvider databaseProvider, Connection connection, PrintStream out) throws Exception {
      int[] columnSizes = {10, 10, 30, 15, 20, 90};
      TableOut tableOut = new TableOut("|", 2, columnSizes);

      tableOut.print(out, new String[]{"ID", "Address", "Name", "Type", "Filter", "Config"});

      AtomicInteger count = new AtomicInteger();
      QueueJDBCQuery query = new QueueJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         String filter = data.filter != null ? data.filter : "";
         String config = data.queueConfigJson != null ? data.queueConfigJson : "";
         tableOut.print(out, new String[]{
            String.valueOf(data.id),
            String.valueOf(data.addressId),
            data.name,
            data.getQueueTypes(),
            filter,
            config
         });
         count.incrementAndGet();
      });
      out.println("Total queues: " + count.get());
      out.println();
   }

   private static void printMessages(DatabaseProvider databaseProvider, Connection connection, PrintStream out, boolean safe) throws Exception {
      int[] columnSizes = {10, 10, 120};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"ID", "TX", safe ? "Size" : "Message"});

      AtomicInteger count = new AtomicInteger();
      MessagesJDBCQuery query = new MessagesJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         String txStr = data.tx != null ? String.valueOf(data.tx) : "";
         if (safe) {
            int size = data.messageBufferSupplier != null ? data.messageBufferSupplier.get().readableBytes() : 0;
            tableOut.print(out, new String[]{String.valueOf(data.messageID), txStr, size + " bytes"});
         } else {
            try {
               Message message = MessagePersister.getInstance().decode(data.messageBufferSupplier.get(), null, null);
               message.setMessageID(data.messageID);
               tableOut.print(out, new String[]{String.valueOf(data.messageID), txStr, String.valueOf(message)});
            } catch (Exception e) {
               tableOut.print(out, new String[]{String.valueOf(data.messageID), txStr, "ERROR decoding: " + e.getMessage()});
            }
         }
         count.incrementAndGet();
      });
      out.println("Total messages: " + count.get());
      out.println();
   }

   private static void printReferences(DatabaseProvider databaseProvider, Connection connection, PrintStream out) throws Exception {
      int[] columnSizes = {10, 10};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"Msg ID", "Queue"});

      AtomicInteger count = new AtomicInteger();
      ReferencesJDBCQuery query = new ReferencesJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         tableOut.print(out, new String[]{String.valueOf(data.messageID), String.valueOf(data.queueID)});
         count.incrementAndGet();
      });
      out.println("Total references: " + count.get());
      out.println();
   }

   private static void printPages(DatabaseProvider databaseProvider, Connection connection, PrintStream out, boolean safe) throws Exception {
      int[] columnSizes = {10, 10, 10, 10, 10, safe ? 10 : 120};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"Address", "Page", "Seq", "Msg ID", "TX", safe ? "Size" : "Message"});

      AtomicInteger count = new AtomicInteger();
      PageJDBCQuery query = new PageJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         String txStr = data.txID != null ? String.valueOf(data.txID) : "";
         if (safe) {
            int size = data.messageBufferSupplier != null ? data.messageBufferSupplier.get().readableBytes() : 0;
            tableOut.print(out, new String[] {String.valueOf(data.addressID), String.valueOf(data.pageID), String.valueOf(data.pageNR), String.valueOf(data.messageID), txStr, String.valueOf(size)});
         } else {
            try {
               Message message = MessagePersister.getInstance().decode(data.messageBufferSupplier.get(), null, null);
               message.setMessageID(data.messageID);
               tableOut.print(out, new String[] {String.valueOf(data.addressID), String.valueOf(data.pageID), String.valueOf(data.pageNR), String.valueOf(data.messageID), txStr, String.valueOf(message)});
            } catch (Exception e) {
               tableOut.print(out, new String[] {String.valueOf(data.addressID), String.valueOf(data.pageID), String.valueOf(data.pageNR), String.valueOf(data.messageID), txStr, e.getMessage()});
            }
         }
         count.incrementAndGet();
      });
      out.println("Total pages: " + count.get());
      out.println();
   }

   private static void printPageRefs(DatabaseProvider databaseProvider, Connection connection, PrintStream out) throws Exception {
      int[] columnSizes = {10, 10, 10, 10};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"Address", "Page", "Seq", "Queue"});

      AtomicInteger count = new AtomicInteger();
      PageRefJDBCQuery query = new PageRefJDBCQuery(databaseProvider, connection);
      query.query(data -> {
         tableOut.print(out, new String[]{String.valueOf(data.addressID), String.valueOf(data.pageID), String.valueOf(data.pageNR), String.valueOf(data.queueID)});
         count.incrementAndGet();
      });
      out.println("Total page references: " + count.get());
      out.println();
   }

   private static void printGenericData(DatabaseProvider databaseProvider, Connection connection, PrintStream out, boolean bindings) throws Exception {
      String tableName = bindings ? databaseProvider.getSqlProvider().getConfigData() : databaseProvider.getSqlProvider().getBrokerData();

      int[] columnSizes = {10, 40, 10, 80};
      TableOut tableOut = new TableOut("|", 2, columnSizes);
      tableOut.print(out, new String[]{"ID", "Record Type", "TX", "Decoded"});

      AtomicInteger count = new AtomicInteger();
      GenericDataJDBCQuery query = new GenericDataJDBCQuery(databaseProvider, connection, tableName);
      query.query(data -> {
         Object decoded = DatabaseStorageManager.describeGenericData(data.id, data.recordType, data.dataSupplier != null ? data.dataSupplier.get() : null);
         String txStr = data.txId != null ? String.valueOf(data.txId) : "";
         String decodedStr = decoded != null ? decoded.toString() : "UNKNOWN(type=" + data.recordType + ")";
         String typeStr = data.recordType + " (" + JournalRecordIds.recordTypeName(data.recordType) + ")";
         tableOut.print(out, new String[]{String.valueOf(data.id), typeStr, txStr, decodedStr});
         count.incrementAndGet();
      });
      out.println("Total generic data records: " + count.get());
      out.println();
   }

   private static void printBanner(PrintStream out, String title) {
      out.println();
      out.println("********************************************");
      out.println(title);
      out.println("********************************************");
   }
}
