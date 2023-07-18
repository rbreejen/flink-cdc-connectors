/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertNotNull;

/** IT tests for {@link OracleSourceBuilder.OracleIncrementalSource}. */
public class OracleSourceITCase extends OracleSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceITCase.class);

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testOracleParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"CUSTOMERS"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testTaskManagerFailoverInBinlogPhase() throws Exception {
        testOracleParallelSource(FailoverType.TM, FailoverPhase.BINLOG, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testOracleParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInBinlogPhase() throws Exception {
        testOracleParallelSource(FailoverType.JM, FailoverPhase.BINLOG, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testOracleParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"CUSTOMERS"});
    }

    private void testOracleParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testOracleParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testOracleParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        String sourceDDL =
                format(
                        "CREATE TABLE sample_data ("
                                + " ID INT,"
                                + " CREATED_ON DATE,"
                                + " RANDOM_NUMBER INT,"
                                + " primary key (ID) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = 'localhost',"
                                + " 'port' = '1521',"
                                + " 'username' = 'C##MYUSER',"
                                + " 'password' = 'mypassword',"
                                + " 'database-name' = 'XE',"
                                + " 'schema-name' = 'DEBEZIUM',"
                                + " 'table-name' = 'SAMPLE_DATA',"
                                + " 'debezium.database.pdb.name' = 'XEPDB1'"
                                + ")"
                        );

        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from sample_data");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        LOG.info("snapshot data start");
        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the binlog data
        for (String tableId : captureCustomerTables) {
            makeFirstPartBinlogEvents(ORACLE_SCHEMA + '.' + tableId);
        }
        if (failoverPhase == FailoverPhase.BINLOG) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartBinlogEvents(ORACLE_SCHEMA + '.' + tableId);
        }

        String[] binlogForSingleTable =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(Arrays.asList(binlogForSingleTable));
        }
        assertEqualsInAnyOrder(expectedBinlogData, fetchRows(iterator, expectedBinlogData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private void makeFirstPartBinlogEvents(String tableId) throws Exception {
        executeSql("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103");
        executeSql("DELETE FROM " + tableId + " where id = 102");
        executeSql("INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')");
        executeSql("UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
    }

    private void makeSecondPartBinlogEvents(String tableId) throws Exception {
        executeSql("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
        executeSql("INSERT INTO " + tableId + " VALUES(2001, 'user_22','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2002, 'user_23','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2003, 'user_24','Shanghai','123567891234')");
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private void executeSql(String sql) throws Exception {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    /** The type of failover. */
    private enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    private enum FailoverPhase {
        SNAPSHOT,
        BINLOG,
        NEVER
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                ORACLE_CONTAINER.getJdbcUrl(), ORACLE_SYSTEM_USER, ORACLE_SYSTEM_PASSWORD);
    }
}
