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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.db.DerbyDatabase;
import edu.uci.ics.crawler4j.db.DerbyEnvironment;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */
public class Counters {
    private static final Logger logger = LoggerFactory.getLogger(Counters.class);

    public static class ReservedCounterNames {
        public static final String SCHEDULED_PAGES = "Scheduled-Pages";
        public static final String PROCESSED_PAGES = "Processed-Pages";
    }

    private static final String DATABASE_NAME = "Statistics";
    protected DerbyDatabase statisticsDB = null;
    protected DerbyEnvironment env;
    private CrawlConfig config;

    protected final Object mutex = new Object();

    protected Map<String, Long> counterValues;

    public Counters(DerbyEnvironment env, CrawlConfig config) {
        this.env = env;
        this.counterValues = new HashMap<>();
        this.config = config;

    /*
     * When crawling is set to be resumable, we have to keep the statistics
     * in a transactional database to make sure they are not lost if crawler
     * is crashed or terminated unexpectedly.
     */
        if (config.isResumableCrawling()) {
            DerbyEnvironment.DerbyDatabaseConfig dbConfig = new DerbyEnvironment.DerbyDatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            dbConfig.setDeferredWrite(false);
            statisticsDB = env.openDatabase(DATABASE_NAME, dbConfig);

            try {
                List<DerbyDatabase.DerbyCursorEntry> entries = statisticsDB.getAllCounters();
                for (DerbyDatabase.DerbyCursorEntry entry : entries) {
                    if (entry.getValue().length > 0) {
                        String name = entry.getKey();
                        long counterValue = Util.byteArray2Long(entry.getValue());
                        counterValues.put(name, counterValue);
                    }
                }
            } catch (SQLException e) {
                logger.error("Failed to load counters from database", e);
            }
        }
    }

    public long getValue(String name) {
        synchronized (mutex) {
            Long value = counterValues.get(name);
            if (value == null) {
                return 0;
            }
            return value;
        }
    }

    public void setValue(String name, long value) {
        synchronized (mutex) {
            try {
                counterValues.put(name, value);
                if (statisticsDB != null) {
                    statisticsDB.setCounter(name, value);
                }
            } catch (SQLException e) {
                if (config.isHaltOnError()) {
                    throw new RuntimeException(e);
                } else {
                    logger.error("Exception setting value", e);
                }
            }
        }
    }

    public void increment(String name) {
        increment(name, 1);
    }

    public void increment(String name, long addition) {
        synchronized (mutex) {
            long prevValue = getValue(name);
            setValue(name, prevValue + addition);
        }
    }

    public void close() {
        try {
            if (statisticsDB != null) {
                statisticsDB.close();
            }
        } catch (Exception e) {
            logger.error("Exception thrown while trying to close statisticsDB", e);
        }
    }
}