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

package org.apache.activemq.artemis.tests.compatibility.realServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.activemq.artemis.utils.RealServerTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatBaseTest extends RealServerTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected static void unzipJava(File zipFilePath, File dir, File zipTarget) throws Exception {
      // create output directory if it doesn't exist
      deleteDirectory(zipTarget);

      if(!dir.exists()) {
         dir.mkdirs();
      }

      FileInputStream fis;
      //buffer for read and write data to file
      byte[] buffer = new byte[1024];
      fis = new FileInputStream(zipFilePath);
      ZipInputStream zis = new ZipInputStream(fis);
      ZipEntry ze = zis.getNextEntry();
      while(ze != null){
         String fileName = ze.getName();
         File newFile = new File(dir, fileName);
         logger.info("Unzipping to {}", newFile.getAbsolutePath());
         if (ze.isDirectory()) {
            newFile.mkdirs();
         } else {
            //create directories for sub directories in zip
            new File(newFile.getParent()).mkdirs();
            FileOutputStream fos = new FileOutputStream(newFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
               fos.write(buffer, 0, len);
            }
            fos.close();
            //close this ZipEntry
            if (newFile.getName().equals("artemis")) {
               newFile.setExecutable(true);
            }
         }
         zis.closeEntry();
         ze = zis.getNextEntry();
      }
      //close last ZipEntry
      zis.closeEntry();
      zis.close();
      fis.close();

   }



}
