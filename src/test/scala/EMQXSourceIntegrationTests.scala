import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.FunSuite
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URL
import scala.io.Source

class GenericContainerSpec extends FunSuite with TestContainerForAll {

  override val containerDef = GenericContainer.Def(
    "emqx/emqx-enterprise:5.10.0",
    exposedPorts = Seq(18083),
    waitStrategy = Wait.forHttp("/status")
  )

  test("smoke test for starting emqx ") {
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
}
