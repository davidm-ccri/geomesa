package geomesa.core.util

import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.Queues
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}

import scala.collection.JavaConversions._

class BatchMultiScanner(in: Scanner,
                        out: BatchScanner,
                        joinFn: java.util.Map.Entry[Key, Value] => AccRange)
  extends Iterable[java.util.Map.Entry[Key, Value]] with AutoCloseable with Logging {

  type E = java.util.Map.Entry[Key, Value]
  val inExecutor  = Executors.newSingleThreadExecutor()
  val outExecutor = Executors.newSingleThreadExecutor()
  val inQ  = Queues.newLinkedBlockingQueue[E](32768)
  val outQ = Queues.newArrayBlockingQueue[E](32768)
  val inDone  = new AtomicBoolean(false)
  val outDone = new AtomicBoolean(false)

  inExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        in.iterator().foreach(inQ.put)
      } finally {
        inDone.set(true)
      }
    }
  })

  def notDone = !inDone.get
  def inQNonEmpty = !inQ.isEmpty

  outExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while(notDone || inQNonEmpty) {
          val entry = inQ.take()
          if(entry != null) {
            val entries = new collection.mutable.ListBuffer[E]()
            inQ.drainTo(entries)
            val ranges = (List(entry) ++ entries).map(joinFn)
            out.setRanges(ranges)
            out.iterator().foreach(e => outQ.put(e))
          }
        }
      } catch {
        case _: InterruptedException =>
      } finally {
        outDone.set(true)
      }
    }
  })

  override def close() = {
    if (!inExecutor.isShutdown) inExecutor.shutdownNow()
    if (!outExecutor.isShutdown) outExecutor.shutdownNow()
    in.close()
    out.close()
  }

  override def iterator: Iterator[E] = new Iterator[E] {

    // must block since we don't know that we'll actually find anything for this itr
    override def hasNext: Boolean = {
      if(prefetch == null)
        findAnother()

      prefetch != null
    }

    var prefetch: E = null

    // Indicate there MAY be one more in the outQ but not for sure
    def mightHaveAnother = !outDone.get || !outQ.isEmpty

    def findAnother() = {
      // poll while we might have another and the prefected is null
      while(mightHaveAnother && prefetch == null) {
        prefetch = outQ.poll(1, TimeUnit.MILLISECONDS)
      }
    }

    override def next(): E = {
      if(prefetch == null)
        findAnother()

      val ret = prefetch
      prefetch = null
      ret
    }
  }
}
