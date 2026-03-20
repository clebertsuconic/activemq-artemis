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

package org.apache.activemq.artemis.core.memory;

import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;

public abstract class AbstractAddressMemoryManager implements AddressMemoryManager {

   protected final SizeAwareMetric size;

   protected long maxSize;

   protected long maxMessages;

   protected AbstractAddressMemoryManager(long maxSize, long maxMessages) {
      this.maxSize = maxSize;
      this.maxMessages = maxMessages;
      this.size = new SizeAwareMetric(maxSize, maxSize, maxMessages, maxMessages);
      size.setUnderCallback(this::onAddressSizeUnder);
      size.setOverCallback(this::onAddressSizeOver);
      size.setOnSizeCallback((delta, sizeOnly) -> onSizeChange(delta, sizeOnly));
   }

   protected void configureSizeMetric(long maxSize, long maxMessages) {
      this.maxSize = maxSize;
      this.maxMessages = maxMessages;
      size.setMax(maxSize, maxSize, maxMessages, maxMessages);
   }

   @Override
   public void addSize(int size, boolean sizeOnly, boolean affectGlobal) {
      long newSize = this.size.addSize(size, sizeOnly, affectGlobal);

      if (newSize < 0) {
         ActiveMQServerLogger.LOGGER.negativeAddressSize(getName().toString(), newSize);
      }
   }

   @Override
   public long getAddressSize() {
      return size.getSize();
   }

   @Override
   public long getAddressElements() {
      return size.getElements();
   }

   protected abstract void onAddressSizeOver();

   protected abstract void onAddressSizeUnder();

   protected abstract void onSizeChange(int size, boolean sizeOnly);

   @Override
   public abstract GlobalMemoryManager getGlobalMemoryManager();

   @Override
   public abstract SimpleString getName();

   @Override
   public abstract QueueMemoryManager getQueueMemoryManager(SimpleString queue, long queueID, Filter filter, boolean durable) throws Exception;

   @Override
   public abstract QueueMemoryManager getQueueMemoryManager(long queueID);

   @Override
   public abstract boolean checkMemory(Runnable runnable, Consumer<AtomicRunnable> blockedCallback);

   @Override
   public abstract boolean checkMemory(boolean runOnFailure, Runnable runnable, Runnable runWhenBlocking, Consumer<AtomicRunnable> blockedCallback);

   @Override
   public void disableCleanup() {
   }

   @Override
   public void enableCleanup() {
   }

   @Override
   public void scheduleCleanup() {
   }

   @Override
   public boolean isRejectingMessages() {
      return false;
   }

   @Override
   public abstract AddressFullMessagePolicy getAddressFullMessagePolicy();

   @Override
   public abstract boolean isStorePaging();

   @Override
   public abstract ArtemisExecutor getExecutor();

   @Override
   public void execute(Runnable runnable) {
      runnable.run();
   }

   @Override
   public abstract long getMaxSize();

   @Override
   public abstract int getMaxReadMessages();

   @Override
   public abstract int getMaxReadBytes();

   @Override
   public abstract int getPrefetchMessages();

   @Override
   public abstract int getPrefetchBytes();
}
