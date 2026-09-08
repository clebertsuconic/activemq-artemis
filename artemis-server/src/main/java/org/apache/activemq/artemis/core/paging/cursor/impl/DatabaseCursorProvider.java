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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;

public class DatabaseCursorProvider implements PageCursorProvider {

   @Override
   public PagedReference newReference(PagedMessage msg, PageSubscription sub) {
      return null;
   }

   @Override
   public PageSubscription getSubscription(long queueId) {
      return null;
   }

   @Override
   public void forEachSubscription(Consumer<PageSubscription> consumer) {
   }

   @Override
   public PageSubscription createSubscription(long queueId, Filter filter, boolean durable) {
      return new DatabaseCursorSubscription();
   }

   @Override
   public void processReload() throws Exception {
   }

   @Override
   public void stop() {
   }

   @Override
   public void counterSnapshot() {
   }

   @Override
   public void flushExecutors() {
   }

   @Override
   public Future<Boolean> scheduleCleanup() {
      return null;
   }

   @Override
   public void disableCleanup() {
   }

   @Override
   public void resumeCleanup() {
   }

   @Override
   public void onPageModeCleared() {
   }

   @Override
   public void close(PageSubscription pageCursorImpl) {
   }

   @Override
   public void checkClearPageLimit() {
   }

   @Override
   public void counterRebuildStarted() {
   }

   @Override
   public void counterRebuildDone() {
   }

   @Override
   public boolean isRebuildDone() {
      return true;
   }
}
