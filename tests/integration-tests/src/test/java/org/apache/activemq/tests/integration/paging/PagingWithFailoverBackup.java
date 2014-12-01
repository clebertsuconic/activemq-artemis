/**
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
package org.apache.activemq.tests.integration.paging;

import org.apache.activemq.tests.util.SpawnedVMSupport;

/**
 * There is no difference between this class and {@link org.apache.activemq.tests.integration.paging.PagingWithFailoverServer}
 * other than helping us identify it on the logs, as it will show with a different name through spawned logs
 *
 * @author Clebert Suconic
 */

public class PagingWithFailoverBackup extends PagingWithFailoverServer
{
   public static Process spawnVM(final String testDir, final int thisPort, final int otherPort) throws Exception
   {
      return SpawnedVMSupport.spawnVM(PagingWithFailoverBackup.class.getName(), testDir, Integer.toString(thisPort), Integer.toString(otherPort), Boolean.toString(true));
   }
}
