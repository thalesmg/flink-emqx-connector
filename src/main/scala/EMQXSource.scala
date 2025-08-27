// flink-core
import org.apache.flink.connector.base.source.reader.SourceReaderBase
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceSplit
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.core.io.InputStatus
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.source.reader.RecordEmitter
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper
// flink-table-common
import org.apache.flink.table.data.RowData
// org.eclipse.paho.mqttv5.client
import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttCallback
import org.eclipse.paho.mqttv5.client.MqttClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties

import java.util.Collections

// val client = startClient("tcp://127.0.0.1:2883", "cid", "t/#", 1)
def startClient(
    brokerUri: String,
    clientid: String,
    topicFilter: String,
    qos: Int
): MqttClient =
  val client = new MqttClient(brokerUri, clientid)
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
  conn_opts.setCleanStart(false)
  conn_opts.setAutomaticReconnect(true)
  client.setCallback(callback)
  client.connect(conn_opts)
  client.subscribe(topicFilter, qos)
  client

class EMQXSourceSplit extends SourceSplit:
  def splitId = "dummy"

// EMQXSource("tcp://127.0.0.1:1883", "cid", "gname", "t/#", 1)
class EMQXSource[OUT](
    brokerUri: String,
    clientid: String,
    topicFilter: String,
    qos: Int,
    deserializer: DeserializationSchema[OUT]
) extends Source[OUT, EMQXSourceSplit, Unit]
    with ResultTypeQueryable[OUT]:
  require(0 <= qos && qos <= 2, "invalid QoS")
  // TODO: define the split as each subscriber of a shared group

  // Member of `Source`
  def createEnumerator(
      enumContext: SplitEnumeratorContext[EMQXSourceSplit]
  ): SplitEnumerator[EMQXSourceSplit, Unit] =
    // for {l <- Thread.currentThread.getStackTrace} do println(l)
    new EMQXSplitEnumertor()

  // Member of `Source`
  def getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

  // Member of `Source`
  def getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer[Unit] =
    null // todo?

  // Member of `Source`
  def getSplitSerializer(): SimpleVersionedSerializer[EMQXSourceSplit] =
    new SimpleSerializer[EMQXSourceSplit]

  // Member of `Source`
  def restoreEnumerator(
      enumContext: SplitEnumeratorContext[EMQXSourceSplit],
      checkpoint: Unit
  ): SplitEnumerator[EMQXSourceSplit, Unit] =
    null // todo?

  // Member of `SourceReaderFactory`
  def createReader(
      readerContext: SourceReaderContext
  ): SourceReader[OUT, EMQXSourceSplit] =
    new EMQXSourceReader(brokerUri, clientid, topicFilter, qos, deserializer)

  // Member of `ResultTypeQueryable`
  def getProducedType: TypeInformation[OUT] =
    deserializer.getProducedType()

  /*
   * SplitEnumerator
   */
  class EMQXSplitEnumertor extends SplitEnumerator[EMQXSourceSplit, Unit]:
    def start(): Unit = ()
    def close(): Unit = ()
    def addReader(subTaskId: Int): Unit = ()
    def addSplitsBack(
        splits: java.util.List[EMQXSourceSplit],
        subTaskId: Int
    ): Unit = ()
    def handleSplitRequest(subTaskId: Int, requesterHostname: String): Unit = ()
    def snapshotState(snapshotId: Long): Unit = ()
  end EMQXSplitEnumertor

  /*
   * SourceReader
   */
  class EMQXSourceReader[OUT](
      brokerUri: String,
      clientid: String,
      topicFilter: String,
      qos: Int,
      deserializer: DeserializationSchema[OUT]
  ) extends SourceReader[OUT, EMQXSourceSplit]:
    val queue: java.util.Queue[OUT] =
      new java.util.concurrent.ConcurrentLinkedQueue()
    var client: MqttClient = null
    val availabilityHelper = new MultipleFuturesAvailabilityHelper(1)

    // Member of `SourceReader`
    def start(): Unit = {
      client = startClient(brokerUri, clientid, topicFilter, qos, deserializer)
      ()
    }

    // Member of `java.lang.AutoCloseable`
    def close(): Unit =
      client.disconnect()
      client.close()

    // Member of `SourceReader`
    def addSplits(splits: java.util.List[EMQXSourceSplit]): Unit = ()

    // Member of `SourceReader`
    def isAvailable(): java.util.concurrent.CompletableFuture[Void] =
      availabilityHelper.resetToUnAvailable()
      // todo?
      availabilityHelper
        .getAvailableFuture()
        .asInstanceOf[java.util.concurrent.CompletableFuture[Void]]

    // Member of `SourceReader`
    def notifyNoMoreSplits(): Unit = ()

    // Member of `SourceReader`
    def pollNext(output: ReaderOutput[OUT]): InputStatus =
      queue.poll match
        case null  => InputStatus.NOTHING_AVAILABLE
        case value => {
          output.collect(value)
          InputStatus.MORE_AVAILABLE
        }

    // Member of `SourceReader`
    def snapshotState(checkpointId: Long): java.util.List[EMQXSourceSplit] =
      Collections.emptyList()

    private def startClient(
        brokerUri: String,
        clientid: String,
        topicFilter: String,
        qos: Int,
        deserializer: DeserializationSchema[OUT]
    ): MqttClient =
      val client = new MqttClient(brokerUri, clientid, null)
      val conn_opts = new MqttConnectionOptions()
      val callback = new MqttCallback() {
        // todo: add logging
        def connectComplete(reconnect: Boolean, uri: String) = ()
        def disconnected(disconnectResponse: MqttDisconnectResponse) = ()
        def authPacketArrived(reasonCode: Int, props: MqttProperties) = ()
        def deliveryComplete(token: IMqttToken) = ()
        def mqttErrorOccurred(err: MqttException) = ()
        def messageArrived(topic: String, msg: MqttMessage) =
          val decoded: OUT = deserializer.deserialize(msg.getPayload)
          queue.add(decoded)
          val cachedPreviousFuture =
            availabilityHelper.getAvailableFuture
              .asInstanceOf[java.util.concurrent.CompletableFuture[Void]]
          cachedPreviousFuture.complete(null)
          ()
      }
      conn_opts.setCleanStart(false)
      conn_opts.setAutomaticReconnect(true)
      client.setCallback(callback)
      client.connect(conn_opts)
      client.subscribe(topicFilter, qos)
      client

  end EMQXSourceReader

end EMQXSource
