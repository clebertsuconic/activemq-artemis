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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.SizeAwareMetric;

public abstract class AbstractGlobalMemoryManager implements GlobalMemoryManager {

   protected final SizeAwareMetric globalSizeMetric;

   protected long maxSize;

   protected long maxMessages;

   protected AbstractGlobalMemoryManager(long maxSize, long maxMessages) {
      this.maxSize = maxSize;
      this.maxMessages = maxMessages;
      this.globalSizeMetric = new SizeAwareMetric(maxSize, maxSize, maxMessages, maxMessages);
      globalSizeMetric.setOverCallback(this::onGlobalSizeOver);
      globalSizeMetric.setUnderCallback(this::onGlobalSizeUnder);
   }

   public SizeAwareMetric getSizeAwareMetric() {
      return globalSizeMetric;
   }

   public void resetMaxSize(long maxSize, long maxMessages) {
      this.maxSize = maxSize;
      this.maxMessages = maxMessages;
      this.globalSizeMetric.setMax(maxSize, maxSize, maxMessages, maxMessages);
   }

   public long getMaxSize() {
      return maxSize;
   }

   public long getMaxMessages() {
      return maxMessages;
   }

   public GlobalMemoryManager addSize(int size, boolean sizeOnly) {
      long newSize = globalSizeMetric.addSize(size, sizeOnly);

      if (newSize < 0) {
         ActiveMQServerLogger.LOGGER.negativeGlobalAddressSize(newSize);
      }

      return this;
   }

   @Override
   public long getGlobalSize() {
      return globalSizeMetric.getSize();
   }

   @Override
   public long getGlobalMessages() {
      return globalSizeMetric.getElements();
   }

   protected abstract void onGlobalSizeOver();

   protected abstract void onGlobalSizeUnder();

   @Override
   public abstract AddressMemoryManager getMemoryAddressManager(SimpleString address) throws Exception;

   @Override
   public void removeAddress(SimpleString address) throws Exception {
   }

}