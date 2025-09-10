package com.emqx.flink.connector;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.Rule;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.emqx.flink.connector.CollectSink;
import com.emqx.flink.connector.EMQXSource;

import org.testcontainers.containers.wait.strategy.Wait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SupplierWithException;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

@Testcontainers
class EMQXSourceIntegrationTests {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitEnumerator.class);

    @Container
    public GenericContainer emqx = new GenericContainer(DockerImageName.parse("emqx/emqx-enterprise:5.10.0"))
            .withExposedPorts(18083, 1883)
            .waitingFor(Wait.forHttp("/status").forPort(18083));

    @Rule
    public final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(1)
                    .setNumberTaskManagers(1)
                    .build());

    MqttClient startClient(String brokerUri) throws Exception {
        MqttClient client = new MqttClient(brokerUri, null, null);
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        MqttCallback callback = new MqttCallback() {
            @Override
            public void connectComplete(boolean reconnect, String uri) {
                System.out.println("connected");
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.out.println("disconnected");
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties props) {
                System.out.println("auth packet arrived");
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                System.out.println("delivery complete");
            }

            @Override
            public void messageArrived(String topic, MqttMessage msg) throws Exception {
                System.out.println(String.format("message arrived: %s", new String(msg.getPayload())));
            }

            @Override
            public void mqttErrorOccurred(MqttException error) {
                System.out.println(String.format("error: %s", error));
            }
        };
        connOpts.setCleanStart(true);
        connOpts.setAutomaticReconnect(false);
        client.setCallback(callback);
        client.connect(connOpts);
        return client;
    }

    @Test
    public void messageDelivery() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        String brokerUri = String.format("tcp://%s:%d", emqx.getHost(), emqx.getMappedPort(1883));
        String clientid = "cid";
        String groupName = "gname";
        String topicFilter = "t/#";
        int qos = 1;
        StringDeserializer deserializer = new StringDeserializer();

        EMQXSource<String> emqxSource = new EMQXSource<String>(brokerUri, clientid, groupName,
                topicFilter,
                qos,
                deserializer);
        DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource, WatermarkStrategy.noWatermarks(),
                "emqx");
        CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
        source.sinkTo(sink);
        JobClient jobClient = env.executeAsync();
        // fixme: this works with the scala version, but not here...
        // CommonTestUtils.waitUntilCondition(() -> jobClient.getJobStatus().get() ==
        // JobStatus.RUNNING, 500L, 5);
        Thread.sleep(500);

        MqttClient client = startClient(brokerUri);
        String topic = "t/1";
        // for debugging
        client.subscribe(topicFilter, qos);
        int[] ns = { 1, 2, 3 };
        for (int n : ns) {
            client.publish(topic, new MqttMessage(String.valueOf(n).getBytes()));
        }
        CommonTestUtils.waitUntilCondition(() -> sink.getCount() == 3, 200L, 5);

        jobClient.cancel();
        client.disconnect();
        client.close();
    }
}
