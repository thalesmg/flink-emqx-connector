import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.TypeHint

class StringDeserializer extends DeserializationSchema[String] {
  def deserialize(raw: Array[Byte]): String = String(raw)

  def isEndOfStream(nextElement: String) = false

  def getProducedType: TypeInformation[String] =
    TypeInformation.of(new TypeHint[String](){})
}
