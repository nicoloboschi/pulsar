/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.jdbc;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Jdbc sink.
 */
@Slf4j
public abstract class JdbcAbstractSink<T> implements Sink<T> {
    // ----- Runtime fields
    protected JdbcSinkConfig jdbcSinkConfig;
    @Getter
    private Connection connection;
    private String jdbcUrl;
    private String tableName;

    private JdbcUtils.TableId tableId;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;
    private PreparedStatement upsertStatement;
    private PreparedStatement deleteStatement;


    protected static final String ACTION_PROPERTY = "ACTION";

    protected JdbcUtils.TableDefinition tableDefinition;

    // for flush
    private LinkedList<Record<T>> incomingList;
    private AtomicBoolean isFlushing;
    private int batchSize;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        jdbcSinkConfig = JdbcSinkConfig.load(config);

        jdbcUrl = jdbcSinkConfig.getJdbcUrl();
        if (jdbcSinkConfig.getJdbcUrl() == null) {
            throw new IllegalArgumentException("Required jdbc Url not set.");
        }

        Properties properties = new Properties();
        String username = jdbcSinkConfig.getUserName();
        String password = jdbcSinkConfig.getPassword();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }


        initDriverClass();
        connection = DriverManager.getConnection(jdbcSinkConfig.getJdbcUrl(), properties);
        connection.setAutoCommit(false);
        log.info("Opened jdbc connection: {}, autoCommit: {}", jdbcUrl, connection.getAutoCommit());

        tableName = jdbcSinkConfig.getTableName();
        tableId = JdbcUtils.getTableId(connection, tableName);
        // Init PreparedStatement include insert, delete, update
        initStatement();

        int timeoutMs = jdbcSinkConfig.getTimeoutMs();
        batchSize = jdbcSinkConfig.getBatchSize();
        incomingList = new LinkedList<>();
        isFlushing = new AtomicBoolean(false);

        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(this::flush, timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void initDriverClass() throws Exception {
        final String driverClassName = JdbcUtils.getDriverClassName(jdbcSinkConfig.getJdbcUrl());
        if (driverClassName != null) {
            Class.forName(driverClassName);
        }
    }

    private void initStatement()  throws Exception {
        List<String> keyList = Lists.newArrayList();
        String key = jdbcSinkConfig.getKey();
        if (key != null && !key.isEmpty()) {
            keyList = Arrays.asList(key.split(","));
        }
        List<String> nonKeyList = Lists.newArrayList();
        String nonKey = jdbcSinkConfig.getNonKey();
        if (nonKey != null && !nonKey.isEmpty()) {
            nonKeyList = Arrays.asList(nonKey.split(","));
        }

        tableDefinition = JdbcUtils.getTableDefinition(connection, tableId, keyList, nonKeyList);
        insertStatement = JdbcUtils.buildInsertStatement(connection, generateInsertQueryStatement());
        if (jdbcSinkConfig.getInsertMode() == JdbcSinkConfig.InsertMode.UPSERT) {
            upsertStatement = JdbcUtils.buildInsertStatement(connection, generateUpsertQueryStatement());
        }
        if (!nonKeyList.isEmpty()) {
            updateStatement = JdbcUtils.buildUpdateStatement(connection, generateUpdateQueryStatement());
        }
        if (!keyList.isEmpty()) {
            deleteStatement = JdbcUtils.buildDeleteStatement(connection, generateDeleteQueryStatement());
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.getAutoCommit()) {
            connection.commit();
        }
        if (insertStatement != null) {
            insertStatement.close();
        }
        if (updateStatement != null) {
            updateStatement.close();
        }
        if (upsertStatement != null) {
            upsertStatement.close();
        }
        if (deleteStatement != null) {
            deleteStatement.close();
        }
        if (flushExecutor != null) {
            flushExecutor.shutdown();
            flushExecutor = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
        log.info("Closed jdbc connection: {}", jdbcUrl);
    }

    @Override
    public void write(Record<T> record) throws Exception {
        int number;
        synchronized (this) {
            incomingList.add(record);
            number = incomingList.size();
        }
        if (number == batchSize) {
            flushExecutor.schedule(() -> flush(), 0, TimeUnit.MILLISECONDS);
        }
    }

    public String generateInsertQueryStatement() {
        return JdbcUtils.buildInsertSql(tableDefinition);
    }

    public String generateUpdateQueryStatement() {
        return JdbcUtils.buildUpdateSql(tableDefinition);
    }

    public abstract String generateUpsertQueryStatement();

    public abstract List<JdbcUtils.ColumnId> getColumnsForUpsert();

    public String generateDeleteQueryStatement() {
        return JdbcUtils.buildDeleteSql(tableDefinition);
    }

    // bind value with a PreparedStetement
    public abstract void bindValue(
        PreparedStatement statement,
        Mutation mutation) throws Exception;

    public abstract Mutation createMutation(Record<T> message);

    @Data
    @AllArgsConstructor
    protected static class Mutation {
        private MutationType type;
        private Function<String, Object> values;
    }
    protected enum MutationType {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }

    private synchronized void flush() {
        // if not in flushing state, do flush, else return;
        if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
            boolean needAnotherRound;
            final List<Record<T>> swapList = new ArrayList<>();

            synchronized (this) {
                if (log.isDebugEnabled()) {
                    log.debug("Starting flush, queue size: {}", incomingList.size());
                }
                final int maxBatchSize = Math.min(incomingList.size(), batchSize);

                for (int i = 0; i < maxBatchSize; i++) {
                    swapList.add(incomingList.removeFirst());
                }
                needAnotherRound = !incomingList.isEmpty() && incomingList.size() >= batchSize;
            }
            long start = System.nanoTime();

            int count = 0;
            try {
                PreparedStatement currentBatch = null;
                final List<Mutation> mutations =
                        swapList.stream().map(r -> {
                            final Mutation mutation = createMutation(r);
                            return mutation;
                        }).collect(Collectors.toList());
                // bind each record value
                PreparedStatement statement = null;
                for (Mutation mutation : mutations) {
                    switch (mutation.getType()) {
                        case DELETE:
                            statement = deleteStatement;
                            break;
                        case UPDATE:
                            statement = updateStatement;
                            break;
                        case INSERT:
                            statement = insertStatement;
                            break;
                        case UPSERT:
                            statement = upsertStatement;
                            break;
                        default:
                            String msg = String.format(
                                    "Unsupported action %s, can be one of %s, or not set which indicate %s",
                                    mutation.getType(), Arrays.toString(MutationType.values()), MutationType.INSERT);
                            throw new IllegalArgumentException(msg);
                    }
                    bindValue(statement, mutation);
                    count += 1;
                    if (jdbcSinkConfig.isUseJdbcBatch()) {
                        if (currentBatch != null && statement != currentBatch) {
                            executeBatch(swapList, currentBatch);
                            if (log.isDebugEnabled()) {
                                log.debug("Flushed {} messages in {} ms", count, (System.nanoTime() - start) / 1000 / 1000);
                            }
                            start = System.nanoTime();
                        } else {
                            statement.addBatch();
                        }
                        currentBatch = statement;
                    } else {
                        statement.execute();
                    }
                }

                if (jdbcSinkConfig.isUseJdbcBatch()) {
                    executeBatch(swapList, currentBatch);
                    if (log.isDebugEnabled()) {
                        log.debug("Flushed {} messages in {} ms", count, (System.nanoTime() - start) / 1000 / 1000);
                    }
                } else {
                    connection.commit();
                    swapList.forEach(Record::ack);
                }
            } catch (Exception e) {
                log.error("Got exception {} after {} ms", e.getMessage(), (System.nanoTime() - start) / 1000 / 1000, e);
                swapList.forEach(Record::fail);
            }

            isFlushing.set(false);
            if (needAnotherRound) {
                flush();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Already in flushing state, will not flush, queue size: {}", incomingList.size());
            }
        }
    }

    private void executeBatch(List<Record<T>> swapList, PreparedStatement statement) throws SQLException {
        final int[] results = statement.executeBatch();
        connection.commit();
        int index = 0;
        for (int r: results) {
            if (r >= 0) {
                swapList.remove(index++).ack();
            } else {
                log.info("got batch return value {}, failing message ", r);
                swapList.remove(index++).fail();
            }
        }
    }

}
