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

package com.ververica.cdc.connectors.oracle;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.utils.OracleTestUtils;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.Properties;
import java.util.stream.Stream;

/** Example Tests for {@link JdbcIncrementalSource}. */
public class OracleChangeEventSourceExampleTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(OracleChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 1000;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .setConfiguration(new Configuration())
                            .withHaLeadershipControl()
                            .build());

    @Test
    public void testConsumingAllEvents() throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.pdb.name","XEPDB1");

        SourceFunction<String> sourceFunction =
                OracleSource.<String>builder()
                        .hostname("localhost")
                        .port(1521)
                        .database("XE")
                        .schemaList("DEBEZIUM") // monitor debezium & c##myuser schema's
                        .tableList("DEBEZIUM.SAMPLE_DATA") // monitor sample_data
                        .username("C##MYUSER")
                        .password("mypassword")
                        .debeziumProperties(debeziumProperties)
//            .startupOptions(StartupOptions.latest())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Disable Enable checkpointing but seems to not have any effect.
        // enable checkpoint
        // env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
        // set the source parallelism to 4
        env.addSource(sourceFunction)
                //.setParallelism(DEFAULT_PARALLELISM)
                .print()
                .setParallelism(1);
        env.execute("Print Oracle Snapshot + RedoLog");
    }
}
