/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.collections;

import java.util.function.Consumer;

public class EmptyList<E> implements LinkedList<E> {


   private static final LinkedList emptyList = new EmptyList();

   public static final <T> LinkedList<T> getEmptyList() {
      return (LinkedList<T>) emptyList;
   }

   private EmptyList() {
   }




   @Override
   public void addHead(E e) {
   }

   @Override
   public void addTail(E e) {
   }

   @Override
   public E get(int position) {
      return null;
   }

   @Override
   public E poll() {
      return null;
   }

   LinkedListIterator<E> emptyIterator = new LinkedListIterator<E>() {
      @Override
      public void repeat() {
      }

      @Override
      public void close() {
      }

      @Override
      public boolean hasNext() {
         return false;
      }

      @Override
      public E next() {
         return null;
      }
   };

   @Override
   public LinkedListIterator<E> iterator() {
      return emptyIterator;
   }

   @Override
   public void clear() {

   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void clearID() {

   }

   @Override
   public void setNodeStore(NodeStore<E> store) {

   }

   @Override
   public E removeWithID(String listID, long id) {
      return null;
   }

   @Override
   public void forEach(Consumer<E> consumer) {

   }
}