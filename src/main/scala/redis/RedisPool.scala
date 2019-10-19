package redis

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import redis.actors.RedisClientActor
import redis.commands.Transactions
import redis.protocol.RedisReply

import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class RedisServer(host: String = "localhost",
                       port: Int = 6379,
                       password: Option[String] = None,
                       db: Option[Int] = None) {

  override def toString: String = {
    //hide password
    s"RedisServer(host: $host, port: $port, db: $db)"
  }
}

case class RedisConnection(actor: ActorRef, active: Ref[Boolean] = Ref(false))

abstract class RedisClientPoolLike(system: ActorSystem, redisDispatcher: RedisDispatcher) {

  protected val log = Logging.getLogger(system, this)
  def redisServerConnections: scala.collection.Map[RedisServer, RedisConnection]
  implicit val executionContext  = system.dispatchers.lookup(redisDispatcher.name)
  private val redisConnectionRef = Ref(Seq.empty[ActorRef])

  def name: String

  /**
    *
    * @param redisCommand
    * @tparam T
    * @return behave nicely with Future helpers like firstCompletedOf or sequence
    */
  def broadcast[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Seq[Future[T]] = {
    redisConnectionPool.map(redisConnection => {
      send(redisConnection, redisCommand)
    })
  }

  protected def send[T](redisConnection: ActorRef, redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T]

  private def getConnectionsActive: Seq[ActorRef] = {
    redisServerConnections.collect {
      case (_, redisConnection) if redisConnection.active.single.get => redisConnection.actor
    }.toVector
  }

  def redisConnectionPool: Seq[ActorRef] = {
    redisConnectionRef.single.get
  }

  def onConnect(redis: RedisCommands, server: RedisServer): Unit = {
    server.password match {
      case Some(p) =>
        redis.auth(p).onComplete {
          case Success(s) =>
            if (s.toBoolean) {
              log.debug(s"AUTH successful on $server")
              server.db.foreach(redis.select)
            } else {
              log.error(s"AUTH failed on $server: ${s.status.utf8String}")
            }
          case Failure(e) =>
            log.error(e, s"AUTH failed on $server")
        }
      case None =>
        server.db.foreach(redis.select)
    }
  }

  protected def onConnectStatus(server: RedisServer, active: Ref[Boolean]): Boolean => Unit = { status: Boolean =>
    if (active.single.compareAndSet(!status, status)) {
      refreshConnections()
    }
  }

  protected def refreshConnections() = {
    log.debug("refreshing connections")
    val actives = getConnectionsActive
    redisConnectionRef.single.set(actives)
  }

  private def getConnectOperations(server: RedisServer): () => Seq[Operation[_, _]] = () => {
    val redis = new BufferedRequest with RedisCommands {
      implicit val executionContext: ExecutionContext = RedisClientPoolLike.this.executionContext
    }
    onConnect(redis, server)
    redis.operations.result()
  }

  /**
    * Disconnect from the server (stop the actor)
    */
  def stop(): Unit = {
    redisConnectionPool.foreach { redisConnection =>
      system stop redisConnection
    }
  }

  protected def makeRedisConnection(server: RedisServer, defaultActive: Boolean = false) = {
    val active = Ref(defaultActive)
    (server, RedisConnection(makeRedisClientActor(server, active), active))
  }

  private def makeRedisClientActor(server: RedisServer, active: Ref[Boolean]): ActorRef = {
    system.actorOf(
      RedisClientActor
        .props(
          new InetSocketAddress(server.host, server.port),
          getConnectOperations(server),
          onConnectStatus(server, active),
          redisDispatcher.name)
        .withDispatcher(redisDispatcher.name),
      name + '-' + Redis.tempName()
    )
  }

}

class RedisClientMutablePool(
  initialServers: Seq[RedisServer],
  val name: String = "RedisClientPool"
)(implicit system: ActorSystem, redisDispatcher: RedisDispatcher = Redis.dispatcher)
    extends RedisClientPoolLike(system, redisDispatcher)
    with RoundRobinPoolRequest
    with RedisCommands {

  override lazy val redisServerConnections = {
    log.debug(s"initializing MutablePool with $initialServers")
    val m = initialServers map { server =>
      makeRedisConnection(server, defaultActive = true)
    }
    collection.mutable.Map(m: _*)
  }

  def addServer(server: RedisServer): Unit = synchronized {
    if (!redisServerConnections.contains(server)) {
      log.debug(s"adding $server")
      redisServerConnections += makeRedisConnection(server)
    } else {
      log.debug(s"$server already known")
    }
  }

  def removeServer(askServer: RedisServer): Unit = synchronized {
    log.debug(s"removing $askServer")
    redisServerConnections.remove(askServer).foreach { redisServerConnection =>
      system stop redisServerConnection.actor
    }
    refreshConnections()
  }

  refreshConnections()
}

class RedisClientPool(redisServers: Seq[RedisServer], val name: String = "RedisClientPool")(
  implicit _system: ActorSystem,
  redisDispatcher: RedisDispatcher = Redis.dispatcher)
    extends RedisClientPoolLike(_system, redisDispatcher)
    with RoundRobinPoolRequest
    with RedisCommands {

  override lazy val redisServerConnections = {
    redisServers.map { server =>
      makeRedisConnection(server, defaultActive = true)
    } toMap
  }

  refreshConnections()
}

class RedisClientMasterSlaves(
  master: RedisServer,
  slaves: Seq[RedisServer]
)(implicit _system: ActorSystem, redisDispatcher: RedisDispatcher = Redis.dispatcher)
    extends RedisCommands
    with Transactions {
  implicit val executionContext = _system.dispatchers.lookup(redisDispatcher.name)

  val masterClient = RedisClient(master.host, master.port, master.password, master.db)

  val slavesClients = new RedisClientPool(slaves)

  override def send[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T] = {
    if (redisCommand.isMasterOnly || slaves.isEmpty) {
      masterClient.send(redisCommand)
    } else {
      slavesClients.send(redisCommand)
    }
  }

  def redisConnection: ActorRef = masterClient.redisConnection
}

case class SentinelMonitoredRedisClientMasterSlaves(
  sentinels: Seq[(String, Int)] = Seq(("localhost", 26379)),
  master: String
)(implicit _system: ActorSystem, redisDispatcher: RedisDispatcher = Redis.dispatcher)
    extends SentinelMonitored(_system, redisDispatcher)
    with ActorRequest
    with RedisCommands
    with Transactions {

  val masterClient: RedisClient = withMasterAddr((ip, port) => {
    RedisClient(ip, port, name = "SMRedisClient")
  })

  val slavesClients: RedisClientMutablePool = withSlavesAddr(slavesHostPort => {
    val slaves = slavesHostPort.map {
      case (ip, port) => RedisServer(ip, port)
    }
    new RedisClientMutablePool(slaves, name = "SMRedisClient")
  })

  val onNewSlave = (ip: String, port: Int) => {
    log.info(s"onNewSlave $ip:$port")
    slavesClients.addServer(RedisServer(ip, port))
  }

  val onSlaveDown = (ip: String, port: Int) => {
    log.info(s"onSlaveDown $ip:$port")
    slavesClients.removeServer(RedisServer(ip, port))
  }

  val onMasterChange = (ip: String, port: Int) => {
    log.info(s"onMasterChange $ip:$port")
    masterClient.reconnect(ip, port)
  }

  /**
    * Disconnect from the server (stop the actors)
    */
  def stop() = {
    masterClient.stop()
    slavesClients.stop()
    sentinelClients.foreach({ case (_, client) => client.stop() })
  }

  def redisConnection: ActorRef = masterClient.redisConnection

  override def send[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T] = {
    if (redisCommand.isMasterOnly || slavesClients.redisConnectionPool.isEmpty) {
      masterClient.send(redisCommand)
    } else {
      slavesClients.send(redisCommand)
    }
  }
}
