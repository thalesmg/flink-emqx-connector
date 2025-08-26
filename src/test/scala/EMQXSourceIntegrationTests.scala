import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.FunSuite
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URL
import scala.io.Source

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.function.SupplierWithException
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.runtime.testutils.CommonTestUtils
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource

import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttCallback
import org.eclipse.paho.mqttv5.client.MqttClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties

class EMQXSourceIntegrationTests extends FunSuite with TestContainerForAll {

  override val containerDef = GenericContainer.Def(
    "emqx/emqx-enterprise:5.10.0",
    exposedPorts = Seq(18083, 1883),
    waitStrategy = Wait.forHttp("/status").forPort(18083)
  )

  override def beforeAll() = {
    super.beforeAll()
  }

  override def beforeEach(context: BeforeEach) = {
    super.beforeEach(context)
    flinkCluster.before()
  }

  override def afterEach(context: AfterEach) = {
    super.afterEach(context)
    flinkCluster.after()
  }

  lazy val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(1)
      .setNumberTaskManagers(1)
      .build())

  def startClient(brokerUri: String): MqttClient =
    val client = new MqttClient(brokerUri, null, null)
    val conn_opts = new MqttConnectionOptions()
    val callback = new MqttCallback() {
      def connectComplete(reconnect: Boolean, uri: String) = println(
        s"connected to $uri"
      )
      def disconnected(disconnectResponse: MqttDisconnectResponse) = println(
        "disconnected"
      )
      def authPacketArrived(reasonCode: Int, props: MqttProperties) = println(
        "auth arrived"
      )
      def deliveryComplete(token: IMqttToken) = println("delivery complete")
      def mqttErrorOccurred(err: MqttException) = println(s"error: $err")
      def messageArrived(topic: String, msg: MqttMessage) = println(
        s"msg: ${String(msg.getPayload())}"
      )
    }
    conn_opts.setCleanStart(true)
    conn_opts.setAutomaticReconnect(false)
    client.setCallback(callback)
    client.connect(conn_opts)
    client

  test("smoke test for starting emqx") {
    withContainers { case emqxContainer: GenericContainer =>
      val status = Source
          .fromInputStream(
            new URL(
              s"http://${emqxContainer.containerIpAddress}:${emqxContainer.mappedPort(18083)}/status"
            ).openConnection().getInputStream
          )
          .mkString
      assert(status.contains("is started"), s"status: $status")
    }
  }

  test("message delivery") {
    withContainers { case emqxContainer: GenericContainer =>
      val env = StreamExecutionEnvironment.getExecutionEnvironment()
      val brokerUri = s"tcp://${emqxContainer.containerIpAddress}:${emqxContainer.mappedPort(1883)}"
      val clientId = "cid"
      val topicFilter = "t/#"
      val qos = 1
      val deserializer = new StringDeserializer()
      val emqxSource = EMQXSource[String](brokerUri, clientId, topicFilter, qos, deserializer)
      val source =
        env
          .fromSource(emqxSource, WatermarkStrategy.noWatermarks(), "emqx")
          .returns(classOf[String])
      val sink = new CollectSink[String]()
      source.sinkTo(sink)
      val jobGraph = env.getStreamGraph().getJobGraph()
      val clusterClient = flinkCluster.getClusterClient()
      val jobId = clusterClient.submitJob(jobGraph).get()
      val client = startClient(brokerUri)
      // for debugging
      client.subscribe(topicFilter, 2)
      val topic = "t/1"
      for {n <- 1 until 4}
        do client.publish(topic, new MqttMessage(n.toString.getBytes))
      CommonTestUtils.waitUntilCondition(new SupplierWithException[java.lang.Boolean, Exception] {
        def get: java.lang.Boolean = sink.getCount == 3
      }, 200L, 5)
      clusterClient.cancel(jobId)
      client.disconnect()
      client.close()
      assert(false, "todo")
    }
  }
}
