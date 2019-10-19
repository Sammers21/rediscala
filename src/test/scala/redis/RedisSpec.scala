package redis

import java.io.{InputStream, OutputStream}
import java.net.Socket
import java.io.IOException
import java.net.ServerSocket


import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

// import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.io.Source
import scala.sys.process.{ProcessIO, _}
import scala.util.Try

object RedisServerHelper {
  val redisHost           = "127.0.0.1"
  val redisServerCmd      = "redis-server"
  val redisCliCmd         = "redis-cli"
  val redisServerLogLevel = ""
}

abstract class RedisHelper extends TestKit(ActorSystem()) with TestBase with BeforeAndAfterAll {
  protected val processLogger = ProcessLogger(line => log.debug(line), line => log.error(line))

  implicit val executionContext = system.dispatchers.lookup(Redis.dispatcher.name)

  override def spanScaleFactor: Double = {
    testKitSettings.TestTimeFactor
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  class RedisManager {

    import RedisServerHelper._

    var processes: Seq[RedisProcess] = Seq.empty

    def newSentinelProcess(masterName: String, masterPort: Int, port: Int = getFreePort) = {
      startProcess(new SentinelProcess(masterName, masterPort, port))
    }

    def newSlaveProcess(masterPort: Int, port: Int = getFreePort) = {
      log.debug(s"starting slave process on $port")
      startProcess(new SlaveProcess(masterPort, port))
    }

    def newRedisProcess(port: Int = getFreePort) = {
      log.debug(s"starting redis process on $port")
      startProcess(new RedisProcess(port))
    }

    private def startProcess(process: RedisProcess): RedisProcess = {
      process.start()
      ensureRedisStarted(redisHost, process.port)
      processes = processes :+ process
      process
    }

    def stopAll() = {
      processes.foreach(_.stop())
    }

  }

  protected def ensureRedisStarted(host: String, port: Int): Unit = {
    val client = RedisClient(host, port)
    eventually {
      client.ping().futureValue
    }
    log.debug(s"redis was started on $port")
  }

  def getFreePort: Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } catch {
      case _: IOException => -1
    } finally {
      socket.close()
    }
  }
}

abstract class RedisStandaloneServer extends RedisHelper {

  val redisManager = new RedisManager()

  val server = redisManager.newRedisProcess()

  val port       = server.port
  lazy val redis = RedisClient(port = port)

  def redisVersion(): Future[Option[RedisVersion]] = redis.info("Server").map { info =>
    info
      .split("\r\n")
      .drop(1)
      .flatMap { line =>
        line.split(":") match {
          case Array(key, value) => List(key -> value)
          case _                 => List.empty
        }
      }
      .find(_._1 == "redis_version")
      .map(_._2.split("\\.") match {
        case Array(major, minor, patch) =>
          RedisVersion(major.toInt, minor.toInt, patch.toInt)
      })
  }

  def withRedisServer[T](block: (Int) => T): T = {
    val serverProcess = redisManager.newRedisProcess()
    val result        = Try(block(serverProcess.port))
    serverProcess.stop()
    result.get
  }

  override def afterAll() = {
    super.afterAll()
    redisManager.stopAll()
  }
}

abstract class RedisSentinelClients(val masterName: String = "mymaster") extends RedisHelper {
  import RedisServerHelper._

  val masterPort    = getFreePort
  val slavePort1    = getFreePort
  val slavePort2    = getFreePort
  val sentinelPort1 = getFreePort
  val sentinelPort2 = getFreePort
  log.debug(s"starting sentinel clients with master port $masterPort, slave1 $slavePort1, slave2 $slavePort2")

  val sentinelPorts = Seq(sentinelPort1, sentinelPort2)

  lazy val redisClient    = RedisClient(port = masterPort)
  lazy val sentinelClient = SentinelClient(port = sentinelPort1)
  lazy val sentinelMonitoredRedisClient =
    SentinelMonitoredRedisClient(
      master = masterName,
      sentinels = Seq((redisHost, sentinelPort1), (redisHost, sentinelPort2)))

  val redisManager = new RedisManager()

  val master = redisManager.newRedisProcess(masterPort)
  val slave1 = redisManager.newSlaveProcess(masterPort, slavePort1)
  val slave2 = redisManager.newSlaveProcess(masterPort, slavePort2)

  val sentinel1 = redisManager.newSentinelProcess(masterName, masterPort, sentinelPort1)
  val sentinel2 = redisManager.newSentinelProcess(masterName, masterPort, sentinelPort2)

  def newSlaveProcess() = {
    redisManager.newSlaveProcess(masterPort)
  }

  def newSentinelProcess() = {
    redisManager.newSentinelProcess(masterName, masterPort)
  }

  override def afterAll() = {
    super.afterAll()
    redisManager.stopAll()
  }

}

abstract class RedisClusterClients() extends RedisHelper {

  import RedisServerHelper._

  var processes: Seq[Process] = Seq.empty
  protected val fileDir       = createTempDirectory().toFile

  def newNode(port: Int) =
    s"$redisServerCmd --port $port --cluster-enabled yes --cluster-config-file nodes-${port}.conf --cluster-node-timeout 30000 --appendonly yes --appendfilename appendonly-${port}.aof --dbfilename dump-${port}.rdb --logfile ${port}.log --daemonize yes"

  val nodePorts = (0 to 5).map(_ => getFreePort)

  override def beforeAll() = {
    log.debug(s"Starting Redis cluster with $nodePorts")
    fileDir.mkdirs()

    processes = nodePorts.map(s => Process(newNode(s), fileDir).run(processLogger))
    val nodes = nodePorts.map(s => redisHost + ":" + s).mkString(" ")

    val createClusterCmd = s"$redisCliCmd --cluster create --cluster-replicas 1 ${nodes}"
    log.debug(createClusterCmd)
    Process(createClusterCmd)
      .run(
        new ProcessIO(
          (writeInput: OutputStream) => {
//            Thread.sleep(2000)
            println("yes")
            writeInput.write("yes\n".getBytes)
            writeInput.flush()
          },
          (processOutput: InputStream) => {
            Source.fromInputStream(processOutput).getLines().foreach { l =>
              log.debug(l)
            }
          },
          (processError: InputStream) => {
            Source.fromInputStream(processError).getLines().foreach { l =>
              log.error(l)
            }
          },
          daemonizeThreads = false
        )
      )
      .exitValue()

    val servers = nodePorts.map { port =>
      RedisServer(host = redisHost, port = port)
    }
    val client = RedisCluster(servers)
    eventually {
      val clusterInfo = client.clusterInfo().futureValue
      clusterInfo("cluster_known_nodes") shouldBe nodePorts.length.toString
    }
    client.stop()
    log.debug(s"RedisCluster started on $nodePorts")
  }

  override def afterAll() = {
    super.afterAll()
    log.debug("Stop begin")
    nodePorts foreach { port => sendToRedis(redisHost, port, "SHUTDOWN NOSAVE\n")}
    processes.foreach(_.destroy())
    log.debug("Stop end")
  }

  protected def sendToRedis(host: String, port: Int, command: String): Unit = {
    val out = new Socket(redisHost, port).getOutputStream
    out.write(command.getBytes)
    out.flush()
  }

  // def deleteDirectory(): Unit = {
  //   val fileStream = Files.newDirectoryStream(fileDir.toPath)
  //   fileStream.iterator().asScala.foreach(Files.delete)
  //   Files.delete(fileDir.toPath)
  // }
}
