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
package org.apache.activemq.artemis.lockmanager;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public interface DistributedLock extends AutoCloseable {

   enum AcquireResult {
      Timeout, Exit, Done
   }

   @FunctionalInterface
   interface ExitCondition {
      /**
       * @return true as long as we should keep running
       */
      boolean keepRunning();
   }

   interface Pauser {
      void idle();

      static Pauser sleep(long idleTime, TimeUnit timeUnit) {
         final long idleNanos = timeUnit.toNanos(idleTime);
         //can fail spuriously but doesn't throw any InterruptedException
         return () -> LockSupport.parkNanos(idleNanos);
      }

      static Pauser noWait() {
         return () -> {
         };
      }
   }

   String getLockId();

   /**
    * @return {@code true} if there is a valid (ie not expired) owner, {@code false} otherwise
    * @throws IllegalStateException if unable to check the lock state
    */
   default boolean isHeld() throws UnavailableStateException {
      return isHeldByCaller();
   }

   /**
    * @return {@code true} if the caller is a valid (ie not expired) owner, {@code false} otherwise
    * @throws IllegalStateException if unable to check the lock state
    */
   boolean isHeldByCaller() throws UnavailableStateException;

   boolean tryLock() throws UnavailableStateException, InterruptedException;

   /**
    * Not reentrant lock acquisition operation (ie {@link #tryLock()}). It tries to acquire the lock until will
    * succeed (ie {@link AcquireResult#Done}) or got interrupted (ie {@link AcquireResult#Exit}). After each failed
    * attempt is performed a {@link Pauser#idle} call.
    *
    * @param exitCondition condition to check if we should keep trying
    * @param pauser pauser to use between attempts
    * @return the result of the acquisition attempt
    * @throws IllegalStateException if the lock state is unavailable
    */
   default AcquireResult tryAcquire(ExitCondition exitCondition, Pauser pauser) {
      while (exitCondition.keepRunning()) {
         try {
            if (tryLock()) {
               return AcquireResult.Done;
            }
         } catch (UnavailableStateException e) {
            throw new IllegalStateException(e);
         } catch (InterruptedException e) {
            return AcquireResult.Exit;
         }
         pauser.idle();
      }
      return AcquireResult.Exit;
   }

   /**
    * Not reentrant lock acquisition operation (ie {@link #tryLock()}). It tries to acquire the lock until will
    * succeed (ie {@link AcquireResult#Done}), got interrupted (ie {@link AcquireResult#Exit}) or exceed
    * {@code tryAcquireTimeoutMillis}. After each failed attempt is performed a {@link Pauser#idle} call. If the
    * specified timeout is <=0 then it behaves as {@link #tryAcquire(ExitCondition, Pauser)}.
    *
    * @param tryAcquireTimeoutMillis timeout in milliseconds
    * @param pauser pauser to use between attempts
    * @param exitCondition condition to check if we should keep trying
    * @return the result of the acquisition attempt
    * @throws IllegalStateException if the lock state is unavailable
    */
   default AcquireResult tryAcquire(long tryAcquireTimeoutMillis, Pauser pauser, ExitCondition exitCondition) {
      if (tryAcquireTimeoutMillis < 0) {
         return tryAcquire(exitCondition, pauser);
      }
      final long timeoutInNanosecond = TimeUnit.MILLISECONDS.toNanos(tryAcquireTimeoutMillis);
      final long startAcquire = System.nanoTime();
      while (exitCondition.keepRunning()) {
         try {
            if (tryLock()) {
               return AcquireResult.Done;
            }
         } catch (UnavailableStateException e) {
            throw new IllegalStateException(e);
         } catch (InterruptedException e) {
            return AcquireResult.Exit;
         }
         if (System.nanoTime() - startAcquire >= timeoutInNanosecond) {
            return AcquireResult.Timeout;
         }
         pauser.idle();
         //check before doing anything if time is expired
         if (System.nanoTime() - startAcquire >= timeoutInNanosecond) {
            return AcquireResult.Timeout;
         }
      }
      return AcquireResult.Exit;
   }

   default boolean tryLock(long timeout, TimeUnit unit) throws UnavailableStateException, InterruptedException {
      // it doesn't make sense to be super fast
      final long TARGET_FIRE_PERIOD_NS = TimeUnit.MILLISECONDS.toNanos(250);
      if (timeout < 0) {
         throw new IllegalArgumentException("timeout cannot be negative");
      }
      Objects.requireNonNull(unit);
      if (timeout == 0) {
         return tryLock();
      }
      final Thread currentThread = Thread.currentThread();
      final long timeoutNs = unit.toNanos(timeout);
      final long start = System.nanoTime();
      final long deadline = start + timeoutNs;
      long expectedNextFireTime = start;
      while (!currentThread.isInterrupted()) {
         long parkNs = expectedNextFireTime - System.nanoTime();
         while (parkNs > 0) {
            LockSupport.parkNanos(parkNs);
            if (currentThread.isInterrupted()) {
               throw new InterruptedException();
            }
            final long now = System.nanoTime();
            parkNs = expectedNextFireTime - now;
         }
         if (tryLock()) {
            return true;
         }
         final long now = System.nanoTime();
         final long remainingTime = deadline - now;
         if (remainingTime <= 0) {
            return false;
         }
         if (remainingTime < TARGET_FIRE_PERIOD_NS) {
            expectedNextFireTime = now;
         } else {
            expectedNextFireTime += TARGET_FIRE_PERIOD_NS;
         }
      }
      throw new InterruptedException();
   }

   void unlock() throws UnavailableStateException;

   /**
    * Perform the release if this lock is held by the caller.
    * This is an alias for {@link #unlock()}.
    *
    * @throws IllegalStateException if the lock state is unavailable
    */
   default void release() {
      try {
         unlock();
      } catch (UnavailableStateException e) {
         throw new IllegalStateException(e);
      }
   }

   /**
    * Not reentrant lock acquisition operation. The lock can be acquired if is not held by anyone (including the caller)
    * or has an expired ownership.
    * This is an alias for {@link #tryLock()}.
    *
    * @return {@code true} if has been acquired, {@code false} otherwise
    * @throws IllegalStateException if the lock state is unavailable
    */
   default boolean tryAcquire() {
      try {
         return tryLock();
      } catch (UnavailableStateException e) {
         throw new IllegalStateException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IllegalStateException(e);
      }
   }

   /**
    * The expiration in milliseconds from the last valid acquisition/renew.
    * @return expiration time in milliseconds
    */
   default long expirationMillis() {
      return Long.MAX_VALUE;
   }

   /**
    * Returns the local expiration time of the lock.
    * Given that many DBMS won't support standard SQL queries to collect CURRENT_TIMESTAMP at milliseconds granularity,
    * this value is stripped of the milliseconds part, making it less optimistic than reality, if >= 0.
    * It's commonly used as a hard deadline for operations, hence is fine to not have high precision.
    *
    * @return local expiration time in milliseconds, or -1 if not held
    */
   default long localExpirationTime() {
      return -1;
   }

   /**
    * It extends the lock expiration (if held) to {@link System#currentTimeMillis()} + {@link #expirationMillis()}.
    *
    * @return {@code true} if the expiration has been moved on, {@code false} otherwise
    * @throws IllegalStateException if the lock state is unavailable
    */
   default boolean renew() {
      return true;
   }

   void addListener(UnavailableLockListener listener);

   void removeListener(UnavailableLockListener listener);

   @FunctionalInterface
   interface UnavailableLockListener {

      void onUnavailableLockEvent();
   }

   @Override
   void close();
}
