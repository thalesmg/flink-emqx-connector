import org.apache.flink.table.data.RowData

import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.connector.sink2.WriterInitContext

import java.util.ArrayList

class CollectSink[OUT] extends Sink[OUT] {
  var results = new ArrayList[OUT]()

  def getCount = results.size()

  def createWriter(context: WriterInitContext) = new CollectSinkWriter()

  class CollectSinkWriter extends SinkWriter[OUT] {
    override def write(element: OUT, context: SinkWriter.Context) =
      results.add(element)

    override def flush(endOfInput: Boolean) = ()

    override def close() = ()
  }
}
