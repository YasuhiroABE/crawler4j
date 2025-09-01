/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.crawler4j.frontier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.db.DerbyDatabase;
import edu.uci.ics.crawler4j.db.DerbyEnvironment;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */
public class WorkQueues {
    private static final Logger logger = LoggerFactory.getLogger(WorkQueues.class);
    protected final DerbyDatabase urlsDB;
    private final DerbyEnvironment env;

    private final boolean resumable;

    private final WebURLDerbyBinding webURLBinding;

    protected final Object mutex = new Object();

    public WorkQueues(DerbyEnvironment env, String dbName, boolean resumable) {
        this.env = env;
        this.resumable = resumable;
        DerbyEnvironment.DerbyDatabaseConfig dbConfig = new DerbyEnvironment.DerbyDatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(resumable);
        dbConfig.setDeferredWrite(!resumable);
        urlsDB = env.openDatabase(dbName, dbConfig);
        webURLBinding = new WebURLDerbyBinding();
    }

    protected DerbyDatabase.DerbyTransaction beginTransaction() {
        return resumable ? urlsDB.beginTransaction() : null;
    }

    protected static void commit(DerbyDatabase.DerbyTransaction tnx) {
        if (tnx != null) {
            tnx.commit();
        }
    }

    public List<WebURL> get(int max) {
        synchronized (mutex) {
            List<WebURL> results = new ArrayList<>(max);
            try {
                List<DerbyDatabase.DerbyCursorEntry> entries = urlsDB.getFirstNEntries(max);
                for (DerbyDatabase.DerbyCursorEntry entry : entries) {
                    if (entry.getValue().length > 0) {
                        results.add(webURLBinding.entryToObject(entry.getValue()));
                    }
                }
            } catch (SQLException e) {
                logger.error("Failed to get URLs from database", e);
            }
            return results;
        }
    }

    public void delete(int count) {
        synchronized (mutex) {
            try {
                urlsDB.deleteFirstNEntries(count);
            } catch (SQLException e) {
                logger.error("Failed to delete URLs from database", e);
            }
        }
    }

    /*
     * The key that is used for storing URLs determines the order
     * they are crawled. Lower key values results in earlier crawling.
     * Here our keys are 6 bytes. The first byte comes from the URL priority.
     * The second byte comes from depth of crawl at which this URL is first found.
     * The rest of the 4 bytes come from the docid of the URL. As a result,
     * URLs with lower priority numbers will be crawled earlier. If priority
     * numbers are the same, those found at lower depths will be crawled earlier.
     * If depth is also equal, those found earlier (therefore, smaller docid) will
     * be crawled earlier.
     */
    protected static String getDatabaseEntryKey(WebURL url) {
        byte[] keyData = new byte[6];
        keyData[0] = url.getPriority();
        keyData[1] = ((url.getDepth() > Byte.MAX_VALUE) ? Byte.MAX_VALUE : (byte) url.getDepth());
        Util.putIntInByteArray(url.getDocid(), keyData, 2);
        return new String(keyData);
    }

    public void put(WebURL url) {
        try {
            byte[] value = webURLBinding.objectToEntry(url);
            urlsDB.put(getDatabaseEntryKey(url), value);
        } catch (SQLException e) {
            logger.error("Failed to put URL in database", e);
        }
    }

    public long getLength() {
        try {
            return urlsDB.count();
        } catch (SQLException e) {
            logger.error("Failed to get database count", e);
            return 0;
        }
    }

    public void close() {
        urlsDB.close();
    }
}