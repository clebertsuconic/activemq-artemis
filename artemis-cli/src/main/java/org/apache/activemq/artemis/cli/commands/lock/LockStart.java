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

package org.apache.activemq.artemis.cli.commands.lock;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import picocli.CommandLine;

@CommandLine.Command(name = "start", description = "List the lock coordinators and their status on the server.")
public class LockStart extends LockAbstract {

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      stat(context);
      return null;
   }

   private void stat(final ActionContext context) throws Exception {
      try (SimpleManagement simpleManagement = new SimpleManagement(brokerURL, user, password).open()) {
         String lockStatus = simpleManagement.getLockStatus();
         context.out.println(lockStatus);
      }
   }

}
