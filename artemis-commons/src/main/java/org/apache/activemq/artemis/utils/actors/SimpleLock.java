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

package org.apache.activemq.artemis.utils.actors;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class SimpleLock implements Lock {

   Semaphore semaphore = new Semaphore(1);

   @Override
   public void lock() {
      try {
         semaphore.acquire();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   @Override
   public void lockInterruptibly() throws InterruptedException {
      semaphore.acquire();

   }

   @Override
   public boolean tryLock() {
      return semaphore.tryAcquire();
   }

   @Override
   public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      return semaphore.tryAcquire(time, unit);
   }

   @Override
   public void unlock() {
      semaphore.release();
   }

   @Override
   public Condition newCondition() {
      throw new UnsupportedOperationException();
   }
}
