package redis

import akka.util.ByteString
import redis.api.clusters.ClusterSlots
import redis.protocol._



/**
  * Created by npeters on 20/05/16.
  */
class RedisClusterTest extends RedisClusterClients {

  var redisCluster: RedisCluster = null
  override def beforeAll(): Unit = {
    super.beforeAll()
    redisCluster = RedisCluster(nodePorts.map(p => RedisServer("127.0.0.1", p)))
  }

  "RedisComputeSlot" should {
    "simple" in {
      RedisComputeSlot.hashSlot("foo") shouldBe 12182
      RedisComputeSlot.hashSlot("somekey") shouldBe 11058
      RedisComputeSlot.hashSlot("somekey3452345325453532452345") shouldBe 15278
      RedisComputeSlot.hashSlot("rzarzaZERAZERfqsfsdQSFD") shouldBe 14258
      RedisComputeSlot.hashSlot("{foo}46546546546") shouldBe 12182
      RedisComputeSlot.hashSlot("foo_312312") shouldBe 5839
      RedisComputeSlot.hashSlot("aazaza{aa") shouldBe 11473
    }
  }

  "clusterSlots" should {
    "encoding" in {
      val clusterSlotsAsByteString = ByteString(new sun.misc.BASE64Decoder().decodeBuffer(
        "KjMNCio0DQo6MA0KOjU0NjANCiozDQokOQ0KMTI3LjAuMC4xDQo6NzAwMA0KJDQwDQplNDM1OTlkZmY2ZTNhN2I5ZWQ1M2IxY2EwZGI0YmQwMDlhODUwYmE1DQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDMNCiQ0MA0KYzBmNmYzOWI2NDg4MTVhMTllNDlkYzQ1MzZkMmExM2IxNDdhOWY1MA0KKjQNCjoxMDkyMw0KOjE2MzgzDQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDINCiQ0MA0KNDhkMzcxMjBmMjEzNTc4Y2IxZWFjMzhlNWYyYmY1ODlkY2RhNGEwYg0KKjMNCiQ5DQoxMjcuMC4wLjENCjo3MDA1DQokNDANCjE0Zjc2OWVlNmU1YWY2MmZiMTc5NjZlZDRlZWRmMTIxOWNjYjE1OTINCio0DQo6NTQ2MQ0KOjEwOTIyDQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDENCiQ0MA0KYzhlYzM5MmMyMjY5NGQ1ODlhNjRhMjA5OTliNGRkNWNiNDBlNDIwMQ0KKjMNCiQ5DQoxMjcuMC4wLjENCjo3MDA0DQokNDANCmVmYThmZDc0MDQxYTNhOGQ3YWYyNWY3MDkwM2I5ZTFmNGMwNjRhMjENCg=="))
      val clusterSlotsAsBulk: DecodeResult[RedisReply] = RedisProtocolReply.decodeReply(clusterSlotsAsByteString)
      val dr: DecodeResult[String] = clusterSlotsAsBulk.map({
        case a: MultiBulk =>
          ClusterSlots().decodeReply(a).toString()
        case _ => "fail"
      })

      val r = dr match {
        case FullyDecoded(decodeValue, _) =>
          decodeValue shouldBe "Vector(ClusterSlot(0,5460,ClusterNode(127.0.0.1,7000,e43599dff6e3a7b9ed53b1ca0db4bd009a850ba5),Stream(ClusterNode(127.0.0.1,7003,c0f6f39b648815a19e49dc4536d2a13b147a9f50), ?)), " +
            "ClusterSlot(10923,16383,ClusterNode(127.0.0.1,7002,48d37120f213578cb1eac38e5f2bf589dcda4a0b),Stream(ClusterNode(127.0.0.1,7005,14f769ee6e5af62fb17966ed4eedf1219ccb1592), ?)), " +
            "ClusterSlot(5461,10922,ClusterNode(127.0.0.1,7001,c8ec392c22694d589a64a20999b4dd5cb40e4201),Stream(ClusterNode(127.0.0.1,7004,efa8fd74041a3a8d7af25f70903b9e1f4c064a21), ?)))"

        case _ => fail()
      }

      r
    }

  }

  "Strings" should {
    "set-get" in {
      log.debug("set")
      redisCluster.set[String]("foo", "FOO").futureValue
      log.debug("exists")
      redisCluster.exists("foo").futureValue shouldBe (true)

      log.debug("get")
      redisCluster.get[String]("foo").futureValue shouldBe Some("FOO")

      log.debug("del")
      redisCluster.del("foo", "foo").futureValue

      log.debug("exists")
      redisCluster.exists("foo").futureValue shouldBe (false)

    }

    "mset-mget" in {
      log.debug("mset")
      redisCluster.mset[String](Map("{foo}1" -> "FOO1", "{foo}2" -> "FOO2")).futureValue
      log.debug("exists")
      redisCluster.exists("{foo}1").futureValue shouldBe (true)
      redisCluster.exists("{foo}2").futureValue shouldBe (true)

      log.debug("mget")
      redisCluster.mget[String]("{foo}1", "{foo}2").futureValue shouldBe Seq(Some("FOO1"), Some("FOO2"))

      log.debug("del")
      redisCluster.del("{foo}1", "{foo}2").futureValue

      log.debug("exists")
      redisCluster.exists("{foo}1").futureValue shouldBe (false)

    }
  }

  "tools" should {
    "groupby" in {
      redisCluster
        .groupByCluserServer(Seq("{foo1}1", "{foo2}1", "{foo1}2", "{foo2}2"))
        .sortBy(_.head)
        .toList shouldBe Seq(Seq("{foo2}1", "{foo2}2"), Seq("{foo1}1", "{foo1}2")).sortBy(_.head)
    }
  }

  "long run" should {
    "wait" in {
      log.debug("set " + redisCluster.getClusterAndConnection(RedisComputeSlot.hashSlot("foo1")).get._1.master.toString)
      redisCluster.set[String]("foo1", "FOO").futureValue
      redisCluster.get[String]("foo1").futureValue
      log.debug("wait...")
      log.debug("get")
      redisCluster.get[String]("foo1").futureValue shouldBe Some("FOO")
    }
  }

  "clusterInfo" should {
    "just work" in {
      val res = redisCluster.clusterInfo().futureValue
      res should not be empty
      for (v <- res) {
        log.debug(s"Key  ${v._1} value ${v._2}")
      }
      res("cluster_state") shouldBe "ok"
    }
  }

  "clusterNodes" should {
    "just work" in {
      val res = redisCluster.clusterNodes().futureValue
      res should not be empty
      for (m <- res) {
        log.debug(m.toString)
      }
      res.size shouldBe 6
      res.count(_.link_state == "connected") shouldBe 6
    }
  }
}
