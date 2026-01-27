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

package org.apache.activemq.artemis.lockmanager.etcd;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.DistributedLockManagerFactory;

public class EtcdDistributedLockManagerFactory implements DistributedLockManagerFactory {

   @Override
   public DistributedLockManager build(Map<String, String> config) {

      EtcdLockConfiguration lockConfiguration = new EtcdLockConfiguration()
         .setUser(config.getOrDefault(USER, DEFAULT_USER))
         .setPassword(config.getOrDefault(PASSWORD, DEFAULT_PASSWORD))
         .setAuthority(config.getOrDefault(AUTHORITY, DEFAULT_AUTHORITY))
         .setLeasePeriod(Integer.parseInt(config.getOrDefault(LEASE_PERIOD, DEFAULT_LEASE_PERIOD)))
         .setEndpoints(config.get(CONNECT_STRING));

      return new EtcdDistributedLockManager(lockConfiguration);
   }

   @Override
   public String getName() {
      return "etcd";
   }

   @Override
   public String getImplName() {
      return EtcdDistributedLockManager.class.getName();
   }

   public static String CONNECT_STRING = "connect-string";
   public static String USER = "user";
   public static String DEFAULT_USER = null;
   public static String PASSWORD = "password";
   public static String DEFAULT_PASSWORD = null;
   public static String AUTHORITY = "authority";
   public static String DEFAULT_AUTHORITY = null;
   public static String LEASE_PERIOD = "lease-period";
   public static String DEFAULT_LEASE_PERIOD = "10";

   public static final Set<String> VALID_PARAMS = Set.of(
      USER, PASSWORD, AUTHORITY, LEASE_PERIOD);

   @Override
   public Set<String> getValidParametersList() {
      return VALID_PARAMS;
   }
}
