package redis.commands

import redis._

import redis.actors.{InvalidRedisReply, ReplyErrorException}
import redis.api.NOSAVE

class ServerSpec extends RedisStandaloneServer {

  "Server commands" should {

    "BGSAVE" in {
      redis.bgsave().futureValue shouldBe "Background saving started"
    }

    "CLIENT KILL" in {
      redis.clientKill("8.8.8.8", 53).failed.futureValue shouldBe a[ReplyErrorException]
    }

    "CLIENT LIST" in {
      redis.clientList().futureValue should not be empty
    }

    "CLIENT GETNAME" in {
      redis.clientGetname().futureValue shouldBe None
    }

    "CLIENT SETNAME" in {
      redis.clientSetname("rediscala").futureValue shouldBe true
    }

    "CONFIG GET" in {
      val map = redis.configGet("*").futureValue
      map should not be empty
    }
    "CONFIG SET" in {
      val r = for {
        set      <- redis.configSet("loglevel", "warning")
        loglevel <- redis.configGet("loglevel")
      } yield {
        set shouldBe true
        loglevel.get("loglevel") shouldBe Some("warning")
      }
      r.futureValue
    }

    "CONFIG RESETSTAT" in {
      redis.configResetstat().futureValue shouldBe true
    }

    "DBSIZE" in {
      redis.dbsize().futureValue should be >= 0L
    }

    "DEBUG OBJECT" in {
      redis.debugObject("serverDebugObj").failed.futureValue shouldBe a[ReplyErrorException]
    }

    "DEBUG SEGFAULT" ignore {}

    "FLUSHALL" in {
      redis.flushall().futureValue shouldBe true
    }

    "FLUSHDB" in {
      redis.flushdb().futureValue shouldBe true
    }

    "INFO" in {
      val r = for {
        info    <- redis.info()
        infoCpu <- redis.info("cpu")
      } yield {
        info shouldBe a[String]
        infoCpu shouldBe a[String]
      }
      r.futureValue
    }

    "LASTSAVE" in {
      redis.lastsave().futureValue should be >= 0L
    }

    "SAVE" in {
      val result = try { redis.save().futureValue } catch {
        case ReplyErrorException("ERR Background save already in progress") => true
      }
      result shouldBe true
    }

    "SLAVE OF" in {
      redis.slaveof("server", 12345).futureValue shouldBe true
    }

    "SLAVE OF NO ONE" in {
      redis.slaveofNoOne().futureValue shouldBe true
    }

    "TIME" in {
      redis.time().futureValue
    }

    "BGREWRITEAOF" in {
      // depending on the redis version, this string could vary, redis 2.8.21 says 'scheduled'
      // but redis 2.8.18 says 'started'
      val r = redis.bgrewriteaof().futureValue
      r should (be("Background append only file rewriting started") or
        be("Background append only file rewriting scheduled"))
    }

    "SHUTDOWN" in {
      redis.shutdown().failed.futureValue shouldBe InvalidRedisReply
    }

    "SHUTDOWN (with modifier)" in {
      withRedisServer(port => {
        val redis = RedisClient(port = port)
        redis.shutdown(NOSAVE).failed.futureValue shouldBe InvalidRedisReply
      })
    }

  }
}
