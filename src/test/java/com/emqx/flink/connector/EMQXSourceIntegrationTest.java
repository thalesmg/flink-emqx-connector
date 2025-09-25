package com.emqx.flink.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.emqx.flink.connector.CollectSink;
import com.emqx.flink.connector.EMQXSource;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import org.testcontainers.containers.wait.strategy.Wait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

@Testcontainers
class EMQXSourceIntegrationTests {
        private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceIntegrationTests.class);

        protected static final AtomicInteger testCount = new AtomicInteger(0);

        @Container
        public static final GenericContainer emqx = new GenericContainer(
                        DockerImageName.parse("emqx/emqx-enterprise:5.10.0"))
                        .withExposedPorts(18083, 1883)
                        .withEnv("EMQX_LOG__CONSOLE_HANDLER__LEVEL", "debug")
                        .waitingFor(Wait.forHttp("/status").forPort(18083));

        @ClassRule
        public final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                        .setNumberSlotsPerTaskManager(3)
                                        .setNumberTaskManagers(1)
                                        .build());

        String mkClientid() {
                return String.format("cid%d-", testCount.incrementAndGet());
        }

        String mkGroupName() {
                return String.format("gname%d-", testCount.get());
        }

        void waitUntilRunning(JobClient jobClient) throws Exception {
                RestClusterClient<?> restClusterClient = flinkCluster.getRestClusterClient();
                CommonTestUtils.waitUntilCondition(() -> {
                        JobStatus jobStatus = jobClient.getJobStatus().get();
                        LOG.info("job {}, status: {}", jobClient.getJobID(), jobStatus);
                        boolean verticesRunning = restClusterClient.getJobDetails(jobClient.getJobID()).get()
                                        .getJobVertexInfos()
                                        .stream()
                                        .allMatch(info -> {
                                                LOG.info("job {}, vertex {}, state: {}",
                                                                jobClient.getJobID(),
                                                                info.getJobVertexID(),
                                                                info.getExecutionState());
                                                return info.getExecutionState() == ExecutionState.RUNNING;
                                        });
                        return jobStatus == JobStatus.RUNNING && verticesRunning;
                }, 1_000L, 5);
        }

        Mqtt5Client startClient(String brokerHost, int brokerPort) throws Exception {
                Mqtt5AsyncClient client = Mqtt5Client.builder()
                                .serverHost(brokerHost)
                                .serverPort(brokerPort)
                                .buildAsync();
                client.publishes(MqttGlobalPublishFilter.ALL,
                                (publish) -> LOG.info("received: {}, {}", publish, publish.getPayload()));
                client.connect().join();
                return client;
        }

        @BeforeEach
        public void setUp() throws Exception {
                flinkCluster.before();
        }

        @Test
        public void messageDelivery() throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(3);
                env.enableCheckpointing(500);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                int qos = 1;
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid, groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);
                // Thread.sleep(500);

                Mqtt5Client client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // for debugging
                client.toBlocking().subscribeWith()
                                .topicFilter(topicFilter)
                                .qos(MqttQos.fromCode(qos));
                int[] ns = { 1, 2, 3 };
                for (int n : ns) {
                        client.toBlocking().publishWith()
                                        .topic(topic)
                                        .qos(MqttQos.fromCode(qos))
                                        .payload(String.valueOf(n).getBytes())
                                        .send();
                }
                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == 3, 500L, 5);

                jobClient.cancel();
                client.toBlocking().disconnect();
        }

        @Test
        public void stopWithSavepoint() throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(3);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                int qos = 1;
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid, groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);

                Mqtt5Client client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // for debugging
                client.toBlocking().subscribeWith()
                                .topicFilter(topicFilter)
                                .qos(MqttQos.fromCode(qos));
                List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs) {
                        client.toBlocking().publishWith()
                                        .topic(topic)
                                        .qos(MqttQos.fromCode(qos))
                                        .payload(msg.getBytes())
                                        .send();
                }
                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(), 500L, 5);

                String savepointPath = jobClient
                                .stopWithSavepoint(false, "/tmp/bah", SavepointFormatType.CANONICAL)
                                .get();
                client.toBlocking().disconnect();
        }

        @ParameterizedTest(name = "Message QoS = {arguments}")
        @ValueSource(ints = { 1 })
        public void recoverAfterFailure(int qos) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new CrashingTestEMQXSource<String>(brokerHost, brokerPort, clientid,
                                groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);

                Mqtt5Client client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // for debugging
                client.toBlocking().subscribeWith()
                                .topicFilter(topicFilter)
                                .qos(MqttQos.fromCode(qos));
                List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs) {
                        client.toBlocking().publishWith()
                                        .topic(topic)
                                        .qos(MqttQos.fromCode(qos))
                                        .payload(msg.getBytes())
                                        .send();
                }

                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(), 500L, 5);

                client.toBlocking().disconnect();

                // Trigger crash by checkpointing
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                LOG.warn("triggering crash by stopping with savepoint");
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                assertThrows(ExecutionException.class,
                                () -> jobClient.stopWithSavepoint(
                                                false,
                                                "/tmp/bah",
                                                SavepointFormatType.CANONICAL)
                                                .get());
                jobClient.cancel().join();

                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                LOG.warn("starting new job");
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
                env2.setParallelism(1);
                // Cannot continue from a savepoint because checkpointing crashed.
                // Configuration config = new Configuration();
                // config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
                // env2.configure(config);

                source = env2.fromSource(emqxSource, WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink2 = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink2);

                JobClient jobClient2 = env2.executeAsync();

                waitUntilRunning(jobClient2);

                // Should replay the same un-acked messages as before the crash.
                org.apache.flink.core.testutils.CommonTestUtils.waitUtil(() -> sink2.getCount() == msgs.size(),
                                Duration.ofMillis(2_500L), Duration.ofMillis(500L),
                                String.format("final count: %d", sink2.getCount()));

                jobClient2.cancel().join();
        }

        @Test
        public void startWithBrokerOffline() throws Exception {
                try {
                        emqx.getDockerClient().pauseContainerCmd(emqx.getContainerId()).exec();
                        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        env.setParallelism(1);
                        String brokerHost = emqx.getHost();
                        int brokerPort = emqx.getMappedPort(1883);
                        int qos = 1;
                        String clientid = mkClientid();
                        String groupName = mkGroupName();
                        String topicFilter = "t/#";
                        StringDeserializer deserializer = new StringDeserializer();

                        EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort,
                                        clientid,
                                        groupName,
                                        topicFilter,
                                        qos,
                                        deserializer);
                        DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "emqx");
                        CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                        source.sinkTo(sink);
                        JobClient jobClient = env.executeAsync();

                        Thread.sleep(2_000L);

                        emqx.getDockerClient().unpauseContainerCmd(emqx.getContainerId()).exec();

                        waitUntilRunning(jobClient);

                        Mqtt5Client client = startClient(brokerHost, brokerPort);
                        String topic = "t/1";
                        // for debugging
                        client.toBlocking().subscribeWith()
                                        .topicFilter(topicFilter)
                                        .qos(MqttQos.fromCode(qos));
                        List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf)
                                        .collect(Collectors.toList());
                        for (String msg : msgs) {
                                client.toBlocking().publishWith()
                                                .topic(topic)
                                                .qos(MqttQos.fromCode(qos))
                                                .payload(msg.getBytes())
                                                .send();
                        }

                        CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(), 500L, 5);
                        jobClient.cancel().join();
                        client.toBlocking().disconnect();
                } finally {
                        try {
                                emqx.getDockerClient().unpauseContainerCmd(emqx.getContainerId()).exec();
                        } catch (com.github.dockerjava.api.exception.InternalServerErrorException e) {
                                if (!e.getMessage().contains("is not paused")) {
                                        throw e;
                                }
                        }
                }
        }
}
