package geomesa.core.util

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.{Lists, Queues}
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class BatchMultiScanner(in: Scanner,
                        out: BatchScanner,
                        joinFn: java.util.Map.Entry[Key, Value] => AccRange)
  extends Iterable[java.util.Map.Entry[Key, Value]] {

  type E = java.util.Map.Entry[Key, Value]
  val inExecutor  = Executors.newSingleThreadExecutor()
  val outExecutor = Executors.newSingleThreadExecutor()
  val inQ  = Queues.newArrayBlockingQueue[E](2048)
  val outQ = Queues.newArrayBlockingQueue[E](2048)
  var inDone = new AtomicBoolean(false)
  var outDone = new AtomicBoolean(false)

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

  // TODO parallelize so we can read while consuming the incoming itr - currently this fails tests
  outExecutor.submit(new Runnable {
    override def run(): Unit = {
      // Todo can we not do a list buffer maybe with a fold
      val allRanges = new ListBuffer[AccRange]
      while(notDone || inQNonEmpty) {
        // block until data is ready
        val batch = Lists.newLinkedList[E]()
        val count = inQ.drainTo(batch)
        val ranges = batch.take(count).map(e => joinFn(e))
        allRanges ++= ranges
      }
      out.setRanges(allRanges)
      out.iterator().foreach(outQ.add)
      outDone.set(true)
    }
  })

  override def iterator: Iterator[E] = new Iterator[E] {
    override def hasNext: Boolean = {
      val ret = !(outDone.get && outQ.isEmpty)
      if(!ret) {
        inExecutor.shutdownNow()
        outExecutor.shutdownNow()
      }
      ret
    }

    override def next(): E = outQ.take()
  }
}
