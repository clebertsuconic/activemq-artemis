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

package org.apache.artemis.database.sql;

public abstract class SQLProvider {

   public String getSelectCount(String tableName) {
      return String.format("SELECT COUNT(*) FROM %s", tableName);
   }

   public String getMessages() {
      return "DB_MESSAGES";
   }

   public String getRefs() {
      return "DB_REFERENCES";
   }

   public String getAddress() {
      return "DB_ADDRESS";
   }

   public String getQueue() {
      return "DB_QUEUE";
   }

   public String getPage() {
      return "DB_PAGE";
   }

   public String getPageRefs() {
      return "DB_PAGE_REFERENCES";
   }

   public String getBrokerData() {
      return "DB_BROKER_DATA";
   }

   public String getConfigData() {
      return "DB_CONFIG_DATA";
   }

   public abstract String createMessages(String tableName);

   public abstract String createReferences(String tableName);

   public abstract String createAddress(String tableName);

   public abstract String createQueue(String tableName);

   public abstract String createPage(String tableName);

   public abstract String createPageReferences(String tableName);

   public abstract String createGenericData(String tableName);

   public String insertMessages(String tableName) {
      return String.format("INSERT INTO %s (MESSAGE_ID, MESSAGE_RECORD, TX_ID) VALUES (?,?,?)", tableName);
   }

   public String insertReferences(String tableName) {
      return String.format("INSERT INTO %s (MESSAGE_ID, QUEUE_ID, PENDING_DELIVERY, TX_ID) VALUES (?,?,?,?)", tableName);
   }

   public String deleteReferences(String tableName) {
      return String.format("DELETE FROM %s WHERE QUEUE_ID=? AND MESSAGE_ID=?", tableName);
   }

   public String deleteMessages(String tableName) {
      return String.format("DELETE FROM %s WHERE MESSAGE_ID=?", tableName);
   }

   public String insertPage(String tableName) {
      return String.format("INSERT INTO %s (ADDRESS_ID, PAGE_ID, PAGE_NR, MESSAGE_ID, TX_ID, MESSAGE_RECORD) VALUES (?,?,?,?,?,?)", tableName);
   }

   public String deletePage(String tableName) {
      return String.format("DELETE FROM %s WHERE ADDRESS_ID=? AND PAGE_ID=?", tableName);
   }

   public String insertPageReferences(String tableName) {
      return String.format("INSERT INTO %s (ADDRESS_ID, PAGE_ID, PAGE_NR, QUEUE_ID) VALUES (?,?,?,?)", tableName);
   }

   public String deletePageReferences(String tableName) {
      return String.format("DELETE FROM %s WHERE ADDRESS_ID=? AND PAGE_ID=? AND PAGE_NR=? AND QUEUE_ID=?", tableName);
   }

   public String deleteAllPageReferences(String tableName) {
      return String.format("DELETE FROM %s WHERE ADDRESS_ID=? AND PAGE_ID=?", tableName);
   }

   public String insertAddress(String tableName) {
      return String.format("INSERT INTO %s (ADDRESS_ID, ADDRESS_NAME, IS_MULTICAST, IS_ANYCAST) VALUES (?, ?, ?, ?)", tableName);
   }

   public String deleteAddress(String tableName) {
      return String.format("DELETE FROM %s WHERE ADDRESS_ID=?", tableName);
   }

   public String insertQueue(String tableName) {
      return String.format("INSERT INTO %s (QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST, IS_ANYCAST, FILTER_STRING, QUEUE_CONFIG) VALUES (?, ?, ?, ?, ?, ?, ?)", tableName);
   }

   public String updateQueue(String tableName) {
      return String.format("UPDATE %s SET ADDRESS_ID=?, QUEUE_NAME=?, IS_MULTICAST=?, IS_ANYCAST=?, FILTER_STRING=?, QUEUE_CONFIG=? WHERE QUEUE_ID=?", tableName);
   }

   public String deleteQueue(String tableName) {
      return String.format("DELETE FROM %s WHERE QUEUE_ID=?", tableName);
   }

   public String reloadMessages(String messagesTable, String referencesTable) {
      return String.format("SELECT a.MESSAGE_ID, a.MESSAGE_RECORD FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.MESSAGE_ID = a.MESSAGE_ID AND b.PENDING_DELIVERY='N') ORDER BY a.MESSAGE_ID", messagesTable, referencesTable);
   }

   // returning only the messages that have at least one PENDING_DELIVERY = "N"
   public String orphanedMessages(String messagesTable, String referencesTable) {
      return String.format("SELECT a.MESSAGE_ID, a.MESSAGE_RECORD FROM %s a WHERE a.MESSAGE_ID NOT IN (SELECT b.MESSAGE_ID FROM %s b WHERE a.MESSAGE_ID = b.MESSAGE_ID) ORDER BY MESSAGE_ID", messagesTable, referencesTable);
   }

   public String deliverPendingMessages(String messagesTable, String referencesTable) {
      return String.format("SELECT a.MESSAGE_ID MESSAGE_ID, a.MESSAGE_RECORD MESSAGE_RECORD, b.PENDING_DELIVERY PENDING_DELIVERY FROM %s a, %s b WHERE a.MESSAGE_ID = b.MESSAGE_ID AND b.QUEUE_ID=? AND b.PENDING_DELIVERY='Y' ORDER BY a.MESSAGE_ID", messagesTable, referencesTable);
   }

   public String updatePendingDelivery(String tableName) {
      return String.format("UPDATE %s SET PENDING_DELIVERY='N' WHERE QUEUE_ID=? AND MESSAGE_ID=?", tableName);
   }

   public String selectReferences(String tableName) {
      return String.format("SELECT MESSAGE_ID, QUEUE_ID, PENDING_DELIVERY FROM %s WHERE PENDING_DELIVERY='N' ORDER BY MESSAGE_ID, QUEUE_ID", tableName);
   }

   public String selectAddress(String tableName) {
      return String.format("SELECT ADDRESS_ID, ADDRESS_NAME, IS_MULTICAST, IS_ANYCAST FROM %s ORDER BY ADDRESS_ID", tableName);
   }

   public String selectQueue(String tableName) {
      return String.format("SELECT QUEUE_ID, ADDRESS_ID, QUEUE_NAME, IS_MULTICAST, IS_ANYCAST, FILTER_STRING, QUEUE_CONFIG FROM %s ORDER BY QUEUE_ID", tableName);
   }

   public String selectPage(String tableName) {
      return String.format("SELECT ADDRESS_ID, PAGE_ID, PAGE_NR, MESSAGE_ID, TX_ID, MESSAGE_RECORD FROM %s ORDER BY ADDRESS_ID, PAGE_ID, PAGE_NR", tableName);
   }

   public String selectPageReferences(String tableName) {
      return String.format("SELECT ADDRESS_ID, PAGE_ID, PAGE_NR, QUEUE_ID FROM %s ORDER BY ADDRESS_ID, PAGE_ID, PAGE_NR, QUEUE_ID", tableName);
   }

   public String insertGenericData(String tableName) {
      return String.format("INSERT INTO %s (ID, RECORD_TYPE, TX_ID, DATA_RECORD) VALUES (?,?,?,?)", tableName);
   }

   public String updateGenericData(String tableName) {
      return String.format("UPDATE %s SET DATA_RECORD=?, TX_ID=? WHERE ID=?", tableName);
   }

   public String deleteGenericData(String tableName) {
      return String.format("DELETE FROM %s WHERE ID=?", tableName);
   }

   public String selectGenericData(String tableName) {
      return String.format("SELECT ID, RECORD_TYPE, TX_ID, DATA_RECORD FROM %s ORDER BY ID", tableName);
   }

}
