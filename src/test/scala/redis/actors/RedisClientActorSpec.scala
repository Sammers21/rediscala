package redis.actors

import java.net.InetSocketAddress

import akka.actor._
import akka.testkit._
import akka.util.ByteString
import redis.api.connection.Ping
import redis.api.strings.Get
import redis.{Operation, Redis, TestBase}

import scala.collection.mutable
import scala.concurrent.{Await, Promise}

class RedisClientActorSpec extends TestKit(ActorSystem()) with TestBase with ImplicitSender {

  import scala.concurrent.duration._

  val getConnectOperations: () => Seq[Operation[_, _]] = () => {
    Seq()
  }

  val timeout = 120.seconds dilated

  val onConnectStatus: (Boolean) => Unit = (status: Boolean) => {}

  "RedisClientActor" should {

    "ok" in within(timeout) {
      val probeReplyDecoder = TestProbe()
      val probeMock         = TestProbe()

      val promiseConnect1 = Promise[String]()
      val opConnectPing   = Operation(Ping, promiseConnect1)
      val promiseConnect2 = Promise[Option[ByteString]]()
      val getCmd          = Get("key")
      val opConnectGet    = Operation(getCmd, promiseConnect2)

      val getConnectOperations: () => Seq[Operation[_, _]] = () => {
        Seq(opConnectPing, opConnectGet)
      }

      val redisClientActor = TestActorRef[RedisClientActorMock](
        Props(
          classOf[RedisClientActorMock],
          probeReplyDecoder.ref,
          probeMock.ref,
          getConnectOperations,
          onConnectStatus)
          .withDispatcher(Redis.dispatcher.name))

      val promise = Promise[String]()
      val op1     = Operation(Ping, promise)
      redisClientActor ! op1
      val promise2 = Promise[String]()
      val op2      = Operation(Ping, promise2)
      redisClientActor ! op2

      probeMock.expectMsg(WriteMock) shouldBe WriteMock
      awaitAssert(redisClientActor.underlyingActor.queuePromises.length shouldBe 2)

      //onConnectWrite
      redisClientActor.underlyingActor.onConnectWrite()
      awaitAssert(redisClientActor.underlyingActor.queuePromises.toSeq shouldBe Seq(opConnectPing, opConnectGet, op1, op2))
      awaitAssert(redisClientActor.underlyingActor.queuePromises.length shouldBe 4)

      //onWriteSent
      redisClientActor.underlyingActor.onWriteSent()
      probeReplyDecoder.expectMsgType[QueuePromises] shouldBe QueuePromises(
        mutable.Queue(opConnectPing, opConnectGet, op1, op2))
      awaitAssert(redisClientActor.underlyingActor.queuePromises shouldBe empty)

      //onDataReceived
      awaitAssert(redisClientActor.underlyingActor.onDataReceived(ByteString.empty))
      probeReplyDecoder.expectMsgType[ByteString] shouldBe ByteString.empty

      awaitAssert(redisClientActor.underlyingActor.onDataReceived(ByteString("bytestring")))
      probeReplyDecoder.expectMsgType[ByteString] shouldBe ByteString("bytestring")

      //onConnectionClosed
      val deathWatcher = TestProbe()
      deathWatcher.watch(probeReplyDecoder.ref)
      redisClientActor.underlyingActor.onConnectionClosed()
      deathWatcher.expectTerminated(probeReplyDecoder.ref, 30 seconds) shouldBe a[Terminated]
    }

    "onConnectionClosed with promises queued" in {
      val probeReplyDecoder = TestProbe()
      val probeMock         = TestProbe()

      val redisClientActor = TestActorRef[RedisClientActorMock](
        Props(
          classOf[RedisClientActorMock],
          probeReplyDecoder.ref,
          probeMock.ref,
          getConnectOperations,
          onConnectStatus)
          .withDispatcher(Redis.dispatcher.name)).underlyingActor

      val promise3 = Promise[String]()
      redisClientActor.receive(Operation(Ping, promise3))
      redisClientActor.queuePromises.length shouldBe 1

      val deathWatcher = TestProbe()
      deathWatcher.watch(probeReplyDecoder.ref)

      redisClientActor.onConnectionClosed()
      deathWatcher.expectTerminated(probeReplyDecoder.ref, 30 seconds) shouldBe a[Terminated]
      a[NoConnectionException.type] should be thrownBy Await.result(promise3.future, 10 seconds)
    }

    "replyDecoder died -> reset connection" in {
      val probeReplyDecoder = TestProbe()
      val probeMock         = TestProbe()

      val redisClientActorRef = TestActorRef[RedisClientActorMock](
        Props(
          classOf[RedisClientActorMock],
          probeReplyDecoder.ref,
          probeMock.ref,
          getConnectOperations,
          onConnectStatus)
          .withDispatcher(Redis.dispatcher.name))
      val redisClientActor = redisClientActorRef.underlyingActor

      val promiseSent    = Promise[String]()
      val promiseNotSent = Promise[String]()
      val operation      = Operation(Ping, promiseSent)
      redisClientActor.receive(operation)
      redisClientActor.queuePromises.length shouldBe 1

      redisClientActor.onWriteSent()
      redisClientActor.queuePromises shouldBe empty
      probeReplyDecoder.expectMsgType[QueuePromises] shouldBe QueuePromises(mutable.Queue(operation))

      redisClientActor.receive(Operation(Ping, promiseNotSent))
      redisClientActor.queuePromises.length shouldBe 1

      val deathWatcher = TestProbe()
      deathWatcher.watch(probeReplyDecoder.ref)
      deathWatcher.watch(redisClientActorRef)

      probeReplyDecoder.ref ! Kill
      deathWatcher.expectTerminated(probeReplyDecoder.ref) shouldBe a[Terminated]
      redisClientActor.queuePromises.length shouldBe 1
    }
  }
}

class RedisClientActorMock(probeReplyDecoder: ActorRef,
                           probeMock: ActorRef,
                           getConnectOperations: () => Seq[Operation[_, _]],
                           onConnectStatus: Boolean => Unit)
    extends RedisClientActor(
      new InetSocketAddress("localhost", 6379),
      getConnectOperations,
      onConnectStatus,
      Redis.dispatcher.name) {
  override def initRepliesDecoder() = probeReplyDecoder

  override def preStart(): Unit = {
    // disable preStart of RedisWorkerIO
  }

  override def write(byteString: ByteString): Unit = {
    probeMock ! WriteMock
  }
}

object WriteMock
