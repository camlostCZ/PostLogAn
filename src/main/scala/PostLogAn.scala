/**
  * Created by JN on 27.03.2017.
  */

import scala.collection.mutable
import better.files.{File => BetterFile, _}

case class DeliveryLog(time: String, rcpt: String, relay: String, delay: String, dsn: String, status: String)

case class MessageLog(id: String,
					  qid: String,
					  time: String,
					  from: String,
					  removed: Boolean,
					  delivery: List[DeliveryLog]) {
	override def toString(): String = {
		s"Message ID = $id\nQueue ID = $qid\nTime = $time\nFrom = $from\nRemoved = $removed"
	}
}

object PostLogAn {
	def main(args: Array[String]) = {
		if (args.length == 1) {
			val app = new PostLogAn
			app.run(args(0))
		}
		else
			println(usage)
	}

	val usage =
		"""
		  |Usage: PostLogAn <filename>
		""".stripMargin
}

class PostLogAn {
	/**
	  * Container of log entries.
	  * Key: queue ID
	  * Value: log entry
	  */
	private val queue: mutable.Map[String, MessageLog] = mutable.Map()

	def run(filename: String): Unit = {
		//val line = "Mar 28 00:29:30, 1AEF23456BD: from=<jn@example.net>, message-id=<demo1@example.net>"
		//val ml = parseLogLine(line)
		//println(ml.mkString())

		parseFile(filename)
		queue.foreach(it => {
			println(it._2)
			println()
		})
	}

	def parseFile(filename: String): Unit = {
		val f = BetterFile(filename)
		for (line <- f.lineIterator) {
			val mlog = parseLogLine(line)
			mlog.foreach(it => { queue.put(key = it.qid, value = it) })
		}
	}

	def parseLogLine(line: String): Option[MessageLog] = {
		val patFrom = "^([^,]+), ([^:]+): from=<([^>]+)>, message-id=(<[^>]+>)$".r
		val patTo   = "^([^,]+), ([^:]+): to=<([^>]+)>, dsn=([^,]+), delay=([^,]+)$".r
		val patRem  = "^([^,]+), ([^:]+): removed$".r

		val mlog: Option[MessageLog] = line match {
			case patFrom(time, qid, from, msgId) => updateMsgLogFrom(queue.get(qid), time, qid, from, msgId)
			case patTo(time, qid, rcpt, dsn, delay) => updateMsgLogTo(queue.get(qid), time, qid, rcpt, dsn, delay)
			case patRem(time, qid) => updateMsgLogRem(queue.get(qid), time, qid)
			case _ => None
		}
		mlog
	}

	def updateMsgLog(mlog: Option[MessageLog],
					 time: String,
					 qid: String,
					 from: String,
					 msgId: String,
					 rcpt: String,
					 dsn: String,
					 delay: String,
					 removed: Boolean): Option[MessageLog] = {
		mlog match {
			case Some(obj) => {
				val tm = if (time == "") obj.time else time
				val mid = if (msgId == "") obj.id else msgId
				val sender = if (from == "") obj.from else from
				Some(MessageLog(mid, qid, tm, sender, obj.removed || removed, Nil))
			}
			case _ => Some(MessageLog(msgId, qid, time, from, removed, Nil))
		}
	}

	val updateMsgLogFrom = updateMsgLog(_: Option[MessageLog], _: String, _: String, _: String, _: String, "", "", "", removed = false)
	val updateMsgLogRem = updateMsgLog(_: Option[MessageLog], _: String, _: String, "", "", "", "", "", removed = true)
	val updateMsgLogTo = updateMsgLog(_: Option[MessageLog], _: String, _: String, "", "",  _: String, _: String, _: String, removed = false)
}
