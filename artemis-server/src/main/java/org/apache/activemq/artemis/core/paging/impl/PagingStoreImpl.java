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
package org.apache.activemq.artemis.core.paging.impl;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * File-based {@link org.apache.activemq.artemis.core.paging.PagingStore} implementation.
 * Creates {@link FilePage} instances for page storage.
 */
public class PagingStoreImpl extends AbstractPagingStoreImpl {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final DecimalFormat format = new DecimalFormat("000000000");

   private volatile SequentialFileFactory fileFactory;

   private PageTimedWriter timedWriter;

   private final ScheduledExecutorService scheduledExecutor;

   private final long syncTimeout;

   private final boolean syncNonTransactional;

   public PagingStoreImpl(final SimpleString address,
                          final ScheduledExecutorService scheduledExecutor,
                          final long syncTimeout,
                          final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final SequentialFileFactory fileFactory,
                          final PagingStoreFactory storeFactory,
                          final SimpleString storeName,
                          final AddressSettings addressSettings,
                          final ArtemisExecutor executor,
                          final boolean syncNonTransactional) {
      super(address, scheduledExecutor, syncTimeout, pagingManager,
            storageManager, storeFactory,
            storeName, addressSettings, executor, syncNonTransactional);
      this.fileFactory = fileFactory;
      this.scheduledExecutor = scheduledExecutor;
      this.syncTimeout = syncTimeout;
      this.syncNonTransactional = syncNonTransactional;
      this.timedWriter = createPageTimedWriter(scheduledExecutor, syncTimeout);
   }

   public PagingStoreImpl(final SimpleString address,
                          final ScheduledExecutorService scheduledExecutor,
                          final long syncTimeout,
                          final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final SequentialFileFactory fileFactory,
                          final PagingStoreFactory storeFactory,
                          final SimpleString storeName,
                          final AddressSettings addressSettings,
                          final ArtemisExecutor executor,
                          final boolean syncNonTransactional,
                          final Supplier<Boolean> purgePageFolder) {
      super(address, scheduledExecutor, syncTimeout, pagingManager,
            storageManager, storeFactory,
            storeName, addressSettings, executor, syncNonTransactional,
            purgePageFolder);
      this.fileFactory = fileFactory;
      this.scheduledExecutor = scheduledExecutor;
      this.syncTimeout = syncTimeout;
      this.syncNonTransactional = syncNonTransactional;
      this.timedWriter = createPageTimedWriter(scheduledExecutor, syncTimeout);
   }

   // Extension point for unit tests to replace the creation of the PageTimedWriter
   protected PageTimedWriter createPageTimedWriter(ScheduledExecutorService scheduledExecutor, long syncTimeout) {
      Objects.requireNonNull(scheduledExecutor, "scheduledExecutor");
      Objects.requireNonNull(getExecutor(), "executor");
      PageTimedWriter localWriter = new PageTimedWriter(getPageSizeBytes(), getStorageManager(), this, scheduledExecutor, getExecutor(), syncNonTransactional, syncTimeout);
      localWriter.start();
      return localWriter;
   }

   // for tests, used through an accessor
   protected void replacePagedTimedWriter(PageTimedWriter writer) {
      this.timedWriter = writer;
   }

   public PageTimedWriter getPageTimedWriter() {
      return timedWriter;
   }

   // --- FileFactory methods ---

   protected SequentialFileFactory checkFileFactory() throws Exception {
      SequentialFileFactory factory = fileFactory;
      if (factory == null) {
         factory = getStoreFactory().newFileFactory(getStoreName());
         fileFactory = factory;
      }
      return factory;
   }

   protected SequentialFileFactory getFileFactory() throws Exception {
      checkFileFactory();
      return fileFactory;
   }

   @Override
   public File getFolder() {
      final SequentialFileFactory factoryUsed = this.fileFactory;
      if (factoryUsed != null) {
         return factoryUsed.getDirectory();
      } else {
         return null;
      }
   }

   @Override
   public String getFolderName() {
      return fileFactory.getDirectoryName();
   }

   @Override
   public void ioSync() throws Exception {
      if (!fileFactory.supportsIndividualContext()) {
         Page page = getCurrentPage();
         if (page != null) {
            page.trySync();
         }
      }
   }

   @Override
   public boolean checkPageFileExists(final long pageNumber) {
      if (fileFactory == null) {
         return false;
      }
      String fileName = createFileName(pageNumber);

      try {
         SequentialFileFactory factory = checkFileFactory();
         SequentialFile file = factory.createSequentialFile(fileName);
         return file.exists() && file.size() > 0;
      } catch (Exception ignored) {
         logger.warn("PagingStoreFactory::checkPageFileExists never-throws assumption failed.", ignored);
         return true;
      }
   }

   @Override
   public Page newPageObject(final long pageNumber) throws Exception {
      String fileName = createFileName(pageNumber);

      SequentialFileFactory factory = checkFileFactory();

      SequentialFile file = factory.createSequentialFile(fileName);

      return new FilePage(getStoreName(), getStorageManager(), factory, file, pageNumber);
   }

   public String createFileName(final long pageID) {
      synchronized (format) {
         return format.format(pageID) + ".page";
      }
   }

   private static int getPageIdFromFileName(final String fileName) {
      return Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));
   }

   public int getNumberOfFiles() throws Exception {
      final SequentialFileFactory fileFactory = this.fileFactory;
      if (fileFactory != null) {
         List<String> files = fileFactory.listFiles("page");
         return files.size();
      }

      return 0;
   }

   @Override
   public Collection<Integer> getCurrentIds() throws Exception {
      readLock();
      try {
         List<Integer> ids = new ArrayList<>();
         SequentialFileFactory factory = fileFactory;
         if (factory != null) {
            for (String fileName : factory.listFiles("page")) {
               ids.add(getPageIdFromFileName(fileName));
            }
         }
         return ids;
      } finally {
         readUnlock();
      }
   }

   @Override
   public void sendPages(ReplicationManager replicator, Collection<Integer> pageIds) throws Exception {
      final SequentialFileFactory factory = fileFactory;
      for (Integer id : pageIds) {
         SequentialFile sFile = factory.createSequentialFile(createFileName(id));
         if (!sFile.exists()) {
            continue;
         }
         ActiveMQServerLogger.LOGGER.replicaSyncFile(sFile, sFile.size(), getStoreName());
         replicator.syncPages(sFile, id, getAddress());
      }
   }

   @Override
   protected boolean deleteFolder() {
      SequentialFileFactory sequentialFileFactory = fileFactory;
      try {
         if (sequentialFileFactory != null) {
            List<String> files;
            try {
               files = sequentialFileFactory.listFiles(null);
            } catch (Exception e) {
               sequentialFileFactory.onIOError(e, e.getMessage());
               return false;
            }
            files.forEach(f -> {
               SequentialFile file = sequentialFileFactory.createSequentialFile(f);
               try {
                  logger.debug("Deleting {}", file);
                  file.delete();
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
                  sequentialFileFactory.onIOError(e, e.getMessage(), file.getFileName());
               }
            });
            logger.debug("Deleting directory {}", sequentialFileFactory.getDirectory());
            return deleteFolder(sequentialFileFactory);
         }
         return true;
      } finally {
         this.fileFactory = null;
      }
   }

   private boolean deleteFolder(final SequentialFileFactory deletingFolder) {
      if (!deletingFolder.deleteFolder()) {
         ActiveMQServerLogger.LOGGER.failedPurgingFolder(deletingFolder.getDirectory().getAbsolutePath());
         try {
            List<String> filesStillExisting = deletingFolder.listFiles(null);
            filesStillExisting.forEach(f -> logger.info("File {} still on folder {}", f, deletingFolder.getDirectory().getAbsolutePath()));
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
         return false;
      } else {
         return true;
      }
   }

   // --- Hook method overrides ---

   @Override
   protected void initializePages() throws Exception {
      final SequentialFileFactory fileFactory = this.fileFactory;
      if (fileFactory != null) {

         int pageId = 0;
         setCurrentPageId(pageId);
         assert getCurrentPage() == null;

         List<String> files = fileFactory.listFiles("page");

         setNumberOfPages(files.size());

         checkNumberOfPages();

         long firstPage = Long.MAX_VALUE;
         for (String fileName : files) {
            final int fileId = getPageIdFromFileName(fileName);

            if (fileId > pageId) {
               pageId = fileId;
            }

            if (fileId < firstPage) {
               firstPage = fileId;
            }
         }

         setCurrentPageId(pageId);

         if (firstPage != Long.MAX_VALUE) {
            setFirstPageId(firstPage);
         }

         if (pageId != 0) {
            reloadLivePage(pageId);
         }

         // We will not mark it for paging if there's only a single empty file
         final Page page = getCurrentPage();
         if (page != null && !(getNumberOfPages() == 1 && page.getSize() == 0)) {
            startPaging();
         }

         timedWriter.start();
      }
   }

   @Override
   protected void stopTimedWriter() {
      if (timedWriter != null) {
         timedWriter.stop();
      }
   }

   @Override
   protected void startTimedWriter() {
      if (timedWriter != null) {
         timedWriter.start();
      }
   }

   @Override
   protected boolean hasTimedWriterPendingIO() {
      return timedWriter != null && timedWriter.hasPendingIO();
   }

   @Override
   protected void incrementWriteTask() {
      timedWriter.incrementTask();
   }

   @Override
   protected int submitWriteTask(OperationContext context, PagedMessage pagedMessage, Transaction tx, RouteContextList listCtx, boolean useFlowControl) {
      return timedWriter.addTask(context, pagedMessage, tx, listCtx, useFlowControl);
   }

   @Override
   public void writeFlowControl(int credits) {
      if (timedWriter != null) {
         timedWriter.flowControl(credits);
      }
   }

   @Override
   protected void handlePageLoadError(long pageId, Exception e) {
      if (fileFactory != null) {
         SequentialFile file = fileFactory.createSequentialFile(createFileName(pageId));
         fileFactory.onIOError(e, e.getMessage(), file);
      }
   }

   @Override
   protected void removeFromStoreFactory() {
      if (fileFactory != null) {
         try {
            getStoreFactory().removeFileFactory(fileFactory);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
      }
   }

   @Override
   protected boolean hasStorage() {
      return fileFactory != null;
   }
}
