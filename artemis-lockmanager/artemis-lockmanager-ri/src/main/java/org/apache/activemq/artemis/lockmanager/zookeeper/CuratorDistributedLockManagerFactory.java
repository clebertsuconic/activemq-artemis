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

package org.apache.activemq.artemis.lockmanager.zookeeper;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.DistributedLockManagerFactory;

public class CuratorDistributedLockManagerFactory implements DistributedLockManagerFactory {

   private static final String CONNECT_STRING_PARAM = "connect-string";
   private static final String NAMESPACE_PARAM = "namespace";
   private static final String SESSION_MS_PARAM = "session-ms";
   private static final String SESSION_PERCENT_PARAM = "session-percent";
   private static final String CONNECTION_MS_PARAM = "connection-ms";
   private static final String RETRIES_PARAM = "retries";
   private static final String RETRIES_MS_PARAM = "retries-ms";
   private static final Set<String> VALID_PARAMS = Set.of(CONNECT_STRING_PARAM, NAMESPACE_PARAM, SESSION_MS_PARAM, SESSION_PERCENT_PARAM, CONNECTION_MS_PARAM, RETRIES_PARAM, RETRIES_MS_PARAM);

   // It's 9 times the default ZK tick time ie 2000 ms
   private static final String DEFAULT_SESSION_TIMEOUT_MS = Integer.toString(18_000);
   private static final String DEFAULT_CONNECTION_TIMEOUT_MS = Integer.toString(8_000);
   private static final String DEFAULT_RETRIES = Integer.toString(1);
   private static final String DEFAULT_RETRIES_MS = Integer.toString(1000);
   // why 1/3 of the session? https://cwiki.apache.org/confluence/display/CURATOR/TN14
   private static final String DEFAULT_SESSION_PERCENT = Integer.toString(33);

   @Override
   public Set<String> getValidParametersList() {
      return VALID_PARAMS;
   }

   @Override
   public DistributedLockManager build(Map<String, String> config) {
      validateParameters(config);
      return new CuratorDistributedLockManager(config.get(CONNECT_STRING_PARAM),
                                               config.get(NAMESPACE_PARAM),
                                               Integer.parseInt(config.getOrDefault(SESSION_MS_PARAM, DEFAULT_SESSION_TIMEOUT_MS)),
                                               Integer.parseInt(config.getOrDefault(SESSION_PERCENT_PARAM, DEFAULT_SESSION_PERCENT)),
                                               Integer.parseInt(config.getOrDefault(CONNECTION_MS_PARAM, DEFAULT_CONNECTION_TIMEOUT_MS)),
                                               Integer.parseInt(config.getOrDefault(RETRIES_PARAM, DEFAULT_RETRIES)),
                                               Integer.parseInt(config.getOrDefault(RETRIES_MS_PARAM, DEFAULT_RETRIES_MS)));
   }

   @Override
   public String getName() {
      return "ZK";
   }

   @Override
   public String getImplName() {
      return CuratorDistributedLockManager.class.getName();
   }
}
