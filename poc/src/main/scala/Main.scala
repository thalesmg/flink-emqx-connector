import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource


@main def main(): Unit =
  println("hello")
  // val flinkCluster = new MiniClusterWithClientResource(
  //   new MiniClusterResourceConfiguration.Builder()
  //     .setNumberSlotsPerTaskManager(1)
  //     .setNumberTaskManagers(1)
  //     .build())
  val env = StreamExecutionEnvironment.getExecutionEnvironment()
  // FIXME: add parallelism to clientid
  env.setParallelism(1)
  val brokerUri = s"tcp://127.0.0.1:2883"
  val clientId = "cid"
  val topicFilter = "t/#"
  val qos = 1
  val deserializer = new StringDeserializer()
  val emqxSource = EMQXSource[String](brokerUri, clientId, topicFilter, qos, deserializer)
  val source =
    env
      .fromSource(emqxSource, WatermarkStrategy.noWatermarks(), "emqx")
  val sink = new CollectSink[String]()
  source.sinkTo(sink)
  env.execute("EMQX PoC")
  println("bye")
