/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private Client client;
    private String riverIndexName;
    private String indexName;
    private String typeName;
    private SQLService service;
    private BulkOperation operation;
    private int bulkSize;
    private int maxBulkRequests;
    private TimeValue bulkTimeout;
    private TimeValue poll;
//    private TimeValue interval;
    private String url;
    private String driver;
    private String user;
    private String password;
//    private final String primaryKeys;
    private String sql;
    private int fetchsize;
    private List<Object> variables;
//    private boolean rivertable;
//    private boolean versioning;
    private String rounding;
    private int scale;
    private volatile Thread thread;
    private volatile boolean closed;
    private Date creationDate;
    private boolean initialized;
    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings,
                     @RiverIndexName String riverIndexName, Client client) throws Exception {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
        initialized = false;
        if (settings.settings().containsKey("jdbc")) {
            Map<String, Object> jdbcSettings = (Map<String, Object>) settings.settings().get("jdbc");
            url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), null);
            driver = XContentMapValues.nodeStringValue(jdbcSettings.get("driver"), null);
            user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), null);
            password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), null);
        }

        if ( url == null || driver == null || user == null || password == null ) {
            logger.error("failed: river [{}] should have jdbc section with all the following - {url: <something>, driver: <something>, user: <something>, password: <something>} ", riverIndexName);
            return;
        }

        if ( settings.settings().containsKey("dataCfg")) {
            Map<String, Object> dataCfgSettings = (Map<String, Object>) settings.settings().get("dataCfg");
            rounding = XContentMapValues.nodeStringValue(dataCfgSettings.get("rounding"), null);
            scale = XContentMapValues.nodeIntegerValue(dataCfgSettings.get("scale"), 0);
//            interval = XContentMapValues.nodeTimeValue(dataCfgSettings.get("interval"), TimeValue.timeValueMinutes(60));
            poll = XContentMapValues.nodeTimeValue(dataCfgSettings.get("poll"), TimeValue.timeValueMinutes(60));
//            primaryKeys = XContentMapValues.nodeStringValue(dataCfgSettings.get("primaryKeys"), null);
            sql = XContentMapValues.nodeStringValue(dataCfgSettings.get("sql"), null);
            fetchsize = XContentMapValues.nodeIntegerValue(dataCfgSettings.get("fetchsize"), 0);
            variables = XContentMapValues.extractRawValues("variables", dataCfgSettings);
        }

        if ( sql == null) {
            logger.error("failed: river [{}] should have dataCfg section with all the following - {sql: <something>} ", riverIndexName);
            return;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "jdbc");
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "jdbc");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            maxBulkRequests = XContentMapValues.nodeIntegerValue(indexSettings.get("max_bulk_requests"), 30);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "60s"), TimeValue.timeValueMillis(60000));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(60000);
            }
        } else {
            indexName = "jdbc";
            typeName = "jdbc";
            bulkSize = 100;
            maxBulkRequests = 30;
            bulkTimeout = TimeValue.timeValueMillis(60000);
        }
        service = new SQLService(logger).setPrecision(scale).setRounding(rounding);
        operation = new BulkOperation(client, logger).setIndex(indexName).setType(typeName)
                .setBulkSize(bulkSize).setMaxActiveRequests(maxBulkRequests)
                .setMillisBeforeContinue(bulkTimeout.millis());

        initialized = true;
//        .setAcknowledge(riverName.getName(), rivertable ? service : null);
    }

    @Override
    public void start() {
        if ( ! initialized) {
            return;
        }

        logger.info("starting JDBC connector: URL [{}], driver [{}], sql [{}], river table [{}], indexing to [{}]/[{}], poll [{}]",
                url, driver, sql, indexName, typeName, poll);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            creationDate = new Date();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                creationDate = null;
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC connector").newThread(new JDBCConnector());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing JDBC river");
        thread.interrupt();
        closed = true;
    }

    private class JDBCConnector implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Number version;
                    String digest;
                    Long lastRanUnixTimeStamp;
                    // read state from _custom
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    String metaId = "_custom." + indexName + "." + typeName;
                    GetResponse get = client.prepareGet(riverIndexName, riverName().name(), metaId).execute().actionGet();
                    if (creationDate != null || !get.exists()) {
                        version = 1L;
                        digest = null;
                        lastRanUnixTimeStamp = new java.util.Date(1).getTime();
                    } else {
                        Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
                        if (jdbcState != null) {
                            version = (Number) jdbcState.get("version");
                            version = version.longValue() + 1; // increase to next version
                            digest = (String) jdbcState.get("digest");
                            lastRanUnixTimeStamp = (Long) jdbcState.get("lastRanUpto");
                        } else {
                            throw new IOException("can't retrieve previously persisted state from " + riverIndexName + "/" + riverName().name());
                        }
                    }

                    Long now = new java.sql.Date(System.currentTimeMillis()).getTime() - 1000*60; // minus 1 min for safety purposes

                    Connection connection = service.getConnection(driver, url, user, password, true);
                    PreparedStatement statement = service.prepareStatement(connection, sql);
                    Map<String,Object> variableValues = new HashMap<String, Object>();
                    variableValues.put("$from", new Timestamp(lastRanUnixTimeStamp));
                    variableValues.put("$now", new Timestamp(now));
                    service.bind(statement, variables, variableValues);
                    logger.info("STMT" + statement.toString());
                    ResultSet results = service.execute(statement, fetchsize);
                    Merger merger = new Merger(operation, version.longValue());
                    long rows = 0L;
                    while (service.nextRow(results, merger)) {
                        rows++;
                    }
                    merger.close();
                    service.close(results);
                    service.close(statement);
                    service.close(connection);
                    logger.info("got " + rows + " rows for version " + version.longValue() + ", digest = " + merger.getDigest());
                    // this flush is required before house keeping starts
                    operation.flush();
                    // save state to _custom
                    XContentBuilder builder = jsonBuilder();
                    builder.startObject().startObject("jdbc");
                    if (creationDate != null) {
                        builder.field("created", creationDate);
                    }
                    builder.field("version", version.longValue());
                    builder.field("digest", merger.getDigest());
                    builder.field("lastRanUpto", now);
                    builder.endObject().endObject();
                    client.prepareBulk().add(indexRequest(riverIndexName).type(riverName.name()).id(metaId).source(builder)).execute().actionGet();
//                    house keeping if data has changed
//                    if (digest != null && !merger.getDigest().equals(digest)) {
//                        housekeeper(version.longValue());
//                        // perform outstanding housekeeper bulk requests
//                        operation.flush();
//                    }
                    delay("next run");
                } catch (Exception e) {
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
        }

        private void housekeeper(long version) throws IOException {
            logger.info("housekeeping for version " + version);
            client.admin().indices().prepareRefresh(indexName).execute().actionGet();
            SearchResponse response = client.prepareSearch().setIndices(indexName).setTypes(typeName).setSearchType(SearchType.SCAN).setScroll(TimeValue.timeValueMinutes(10)).setSize(bulkSize).setVersion(true).setQuery(matchAllQuery()).execute().actionGet();
            if (response.timedOut()) {
                logger.error("housekeeper scan query timeout");
                return;
            }
            if (response.failedShards() > 0) {
                logger.error("housekeeper failed shards in scan response: {0}", response.failedShards());
                return;
            }
            String scrollId = response.getScrollId();
            if (scrollId == null) {
                logger.error("housekeeper failed, no scroll ID");
                return;
            }
            boolean done = false;
            // scroll
            long deleted = 0L;
            long t0 = System.currentTimeMillis();
            do {
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
                if (response.timedOut()) {
                    logger.error("housekeeper scroll query timeout");
                    done = true;
                } else if (response.failedShards() > 0) {
                    logger.error("housekeeper failed shards in scroll response: {}", response.failedShards());
                    done = true;
                } else {
                    // terminate scrolling?
                    if (response.hits() == null) {
                        done = true;
                    } else {
                        for (SearchHit hit : response.getHits().getHits()) {
                            // delete all documents with lower version
                            if (hit.getVersion() < version) {
                                operation.delete(hit.getIndex(), hit.getType(), hit.getId());
                                deleted++;
                            }
                        }
                        scrollId = response.getScrollId();
                    }
                }
                if (scrollId != null) {
                    done = true;
                }
            } while (!done);
            long t1 = System.currentTimeMillis();
            logger.info("housekeeper ready, {} documents deleted, took {} ms", deleted, t1 - t0);
        }
    }

//    private class JDBCRiverTableConnector implements Runnable {
//
//        private String[] optypes = new String[]{"create", "index", "delete"};
//
//        @Override
//        public void run() {
//            while (true) {
//                for (String optype : optypes) {
//                    try {
//                        Connection connection = service.getConnection(driver, url, user, password, false);
//                        PreparedStatement statement = service.prepareRiverTableStatement(connection, riverName.getName(), optype, interval.millis());
//                        ResultSet results = service.execute(statement, fetchsize);
//                        Merger merger = new Merger(operation);
//                        long rows = 0L;
//                        while (service.nextRiverTableRow(results, merger)) {
//                            rows++;
//                        }
//                        merger.close();
//                        service.close(results);
//                        service.close(statement);
//                        logger.info(optype + ": got " + rows + " rows");
//                        // this flush is required before next run
//                        operation.flush();
//                        service.close(connection);
//                    } catch (Exception e) {
//                        logger.error(e.getMessage(), e, (Object) null);
//                        closed = true;
//                    }
//                    if (closed) {
//                        return;
//                    }
//                }
//                delay("next run");
//            }
//        }
//    }


    private void delay(String reason) {
        if (poll.millis() > 0L) {
            logger.info("{}, waiting {}, URL [{}] driver [{}] sql [{}]",
                    reason, poll, url, driver, sql);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e1) {
            }
        }
    }
}
