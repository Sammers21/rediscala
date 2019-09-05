package redis.actors

import akka.testkit._
import akka.actor.{ActorRef, ActorSystem, Props}
import java.net.InetSocketAddress

import akka.io.Tcp._
import akka.util.ByteString
import akka.io.Tcp.ErrorClosed
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.io.Tcp.CommandFailed
import redis.{Redis, TestBase}

class RedisWorkerIOSpec extends TestKit(ActorSystem()) with TestBase with ImplicitSender {

  import scala.concurrent.duration._

  val timeout = 120.seconds dilated

  "RedisWorkerIO" should {

    val address = new InetSocketAddress("localhost", 6379)
    "connect CommandFailed then reconnect" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      probeTcp.reply(CommandFailed(connectMsg))
      probeMock.expectMsg(OnConnectionClosed) shouldBe OnConnectionClosed

      withClue("reconnect") {
        val connectMsg = probeTcp.expectMsgType[Connect]
        connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)

        val probeTcpWorker = TestProbe()
        probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

        probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)
      }
    }

    "ok" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      redisWorkerIO ! "PING1"

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent

      redisWorkerIO ! "PING2"
      redisWorkerIO ! "PING3"
      probeTcpWorker.reply(WriteAck)
      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING2PING3"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent

      redisWorkerIO ! "PING"
      probeTcpWorker.expectNoMessage(1 seconds)
      probeTcpWorker.send(redisWorkerIO, WriteAck)
      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
    }

    "reconnect : connected <-> disconnected" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      redisWorkerIO ! "PING1"

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent

      redisWorkerIO ! "PING 2"
      awaitAssert(redisWorkerIO.underlyingActor.bufferWrite.result shouldBe ByteString("PING 2"))
      // ConnectionClosed
      probeTcpWorker.send(redisWorkerIO, ErrorClosed("test"))
      probeMock.expectMsg(OnConnectionClosed) shouldBe OnConnectionClosed
      awaitAssert(redisWorkerIO.underlyingActor.bufferWrite.length shouldBe 0)

      // Reconnect
      val connectMsg2 = probeTcp.expectMsgType[Connect]
      connectMsg2 shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker2 = TestProbe()
      probeTcpWorker2.send(redisWorkerIO, Connected(connectMsg2.remoteAddress, address))
      probeTcpWorker2.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      redisWorkerIO ! "PING1"
      probeTcpWorker2.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
    }

    "onConnectedCommandFailed" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      redisWorkerIO ! "PING1"

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      val msg = probeTcpWorker.expectMsgType[Write]
      msg shouldBe Write(ByteString("PING1"), WriteAck)

      probeTcpWorker.reply(CommandFailed(msg))
      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
    }

    "received" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      redisWorkerIO ! "PING1"

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent

      probeTcpWorker.send(redisWorkerIO, Received(ByteString("PONG")))
      probeMock.expectMsgType[ByteString] shouldBe ByteString("PONG")
    }

    "Address Changed" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, ByteString.empty).withDispatcher(Redis.dispatcher.name))

      redisWorkerIO ! "PING1"

      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker.expectMsgType[Write] shouldBe Write(ByteString("PING1"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
      probeTcpWorker.reply(WriteAck)

      // change adresse
      val address2 = new InetSocketAddress("localhost", 6380)
      redisWorkerIO ! address2

      probeMock.expectMsg(OnConnectionClosed) shouldBe OnConnectionClosed

      redisWorkerIO ! "PING2"

      val connectMsg2 = probeTcp.expectMsgType[Connect]
      connectMsg2 shouldBe Connect(address2, options = SO.KeepAlive(on = true) :: Nil)

      val probeTcpWorker2 = TestProbe()
      probeTcpWorker2.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker2.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker2.expectMsgType[Write] shouldBe Write(ByteString("PING2"), WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
      probeTcpWorker2.reply(WriteAck)

      // receiving data on connection with the sending direction closed
      probeTcpWorker.send(redisWorkerIO, Received(ByteString("PONG1")))
      probeMock.expectMsg(DataReceivedOnClosingConnection) shouldBe DataReceivedOnClosingConnection

      // receiving data on open connection
      probeTcpWorker2.send(redisWorkerIO, Received(ByteString("PONG2")))
      probeMock.expectMsgType[ByteString] shouldBe ByteString("PONG2")

      // close connection
      probeTcpWorker.send(redisWorkerIO, ConfirmedClosed)
      probeMock.expectMsg(ClosingConnectionClosed) shouldBe ClosingConnectionClosed
    }

    "on connect write" in within(timeout){
      val probeTcp = TestProbe()
      val probeMock = TestProbe()
      val onConnectByteString = ByteString("on connect write")

      val redisWorkerIO = TestActorRef[RedisWorkerIOMock](Props(classOf[RedisWorkerIOMock], probeTcp.ref, address, probeMock.ref, onConnectByteString).withDispatcher(Redis.dispatcher.name))


      val connectMsg = probeTcp.expectMsgType[Connect]
      connectMsg shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker = TestProbe()
      probeTcpWorker.send(redisWorkerIO, Connected(connectMsg.remoteAddress, address))

      probeTcpWorker.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker.expectMsgType[Write] shouldBe Write(onConnectByteString, WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent

      redisWorkerIO ! "PING1"
      awaitAssert(redisWorkerIO.underlyingActor.bufferWrite.result shouldBe ByteString("PING1"))

      // ConnectionClosed
      probeTcpWorker.send(redisWorkerIO, ErrorClosed("test"))
      probeMock.expectMsg(OnConnectionClosed) shouldBe OnConnectionClosed

      awaitAssert(redisWorkerIO.underlyingActor.bufferWrite.length shouldBe 0)

      // Reconnect
      val connectMsg2 = probeTcp.expectMsgType[Connect]
      connectMsg2 shouldBe Connect(address, options = SO.KeepAlive(on = true) :: Nil)
      val probeTcpWorker2 = TestProbe()
      probeTcpWorker2.send(redisWorkerIO, Connected(connectMsg2.remoteAddress, address))
      probeTcpWorker2.expectMsgType[Register] shouldBe Register(redisWorkerIO)

      probeTcpWorker2.expectMsgType[Write] shouldBe Write(onConnectByteString, WriteAck)
      probeMock.expectMsg(WriteSent) shouldBe WriteSent
    }
  }
}


class RedisWorkerIOMock(probeTcp: ActorRef, address: InetSocketAddress, probeMock: ActorRef, _onConnectWrite: ByteString) extends RedisWorkerIO(address, (status:Boolean) =>{()} ) {
  override val tcp = probeTcp

  def writing: Receive = {
    case s: String => write(ByteString(s))
  }

  def onConnectionClosed() {
    probeMock ! OnConnectionClosed
  }

  def onDataReceived(dataByteString: ByteString) {
    probeMock ! dataByteString
  }

  def onWriteSent() {
    probeMock ! WriteSent
  }

  def onConnectWrite(): ByteString = _onConnectWrite

  def onDataReceivedOnClosingConnection(dataByteString: ByteString): Unit = probeMock ! DataReceivedOnClosingConnection

  def onClosingConnectionClosed(): Unit = probeMock ! ClosingConnectionClosed
}

object WriteSent

object OnConnectionClosed

object DataReceivedOnClosingConnection

object ClosingConnectionClosed
