/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.jsr107.cache;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;

/**
 * ConfigurationAdapter takes {@link javax.cache.Configuration} and creates equivalent instance of
 * {@link org.infinispan.configuration.cache.Configuration}
 * 
 * @author Vladimir Blagojevic
 * @since 5.3
 */
public class ConfigurationAdapter<K, V> {

   private javax.cache.Configuration<K, V> c;
   private ClassLoader classLoader;

   public ConfigurationAdapter(javax.cache.Configuration<K, V> configuration,
            ClassLoader classLoader) {
      this.c = configuration;
      this.classLoader = classLoader;
   }

   public org.infinispan.configuration.cache.Configuration build() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.storeAsBinary().enabled(c.isStoreByValue());
      cb.classLoader(classLoader);
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);

      switch (c.getTransactionMode()) {
         case NONE:
            cb.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
            break;
         case LOCAL:
            cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
            break;
         case XA:
            //TODO
            break;
         default:
            break;
      }
      switch (c.getTransactionIsolationLevel()) {
         case NONE:
            cb.locking().isolationLevel(org.infinispan.util.concurrent.IsolationLevel.NONE);
            break;
         case READ_UNCOMMITTED:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.READ_UNCOMMITTED);
            break;
         case READ_COMMITTED:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.READ_COMMITTED);
            break;
         case REPEATABLE_READ:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.REPEATABLE_READ);
            break;
         case SERIALIZABLE:
            cb.locking().isolationLevel(org.infinispan.util.concurrent.IsolationLevel.SERIALIZABLE);
            break;
         default:
            break;
      }
      //TODO
      //whatever else is needed
      return cb.build();
   }
}
