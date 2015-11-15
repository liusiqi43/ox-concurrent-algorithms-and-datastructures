import scala.collection.immutable.Queue
import ox.cads.util.ThreadUtil
import ox.cads.testing.{LinTester, DFSLinearizabilityTester}
import scala.util.Random

object Test {
  type C = ox.cads.collection.Queue[Int]
  type LC = LinkedArrayList[Int]
  type LFC = LFLinkedArrayList[Int]
  type S = Queue[Int]
    
  def seqEnqueue(x: Int)(q: S) : (Unit, S) = ((), q.enqueue(x))
  def seqDequeue(q: S) : (Option[Int], S) = {
    if (q.isEmpty) (None, q)
    else {val (r, q1) = q.dequeue; (Some(r), q1)}
  }

  def worker(me: Int , tester : LinTester[S, C]) = {
    val random = new scala.util.Random
    for (i <- 0 until 200) {
      if(random.nextFloat <= 0.3){
        val x = random.nextInt(1000)
        tester.log(me, _.enqueue(x), "enqueue("+x+")", seqEnqueue(x))
      }
      else tester.log(me, _.dequeue, "dequeue", seqDequeue)
    }
  }

  def main(args: Array[String]) {
    val t0 = System.nanoTime
    for (i <- 0 until 1000) {
      val concQueue = new LC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-based queue] time taken: " + (System.nanoTime - t0)/1e6 + "ms")

    val t1 = System.nanoTime
    for (i <- 0 until 1000) {
      val concQueue = new LFC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-free queue] time taken: " + (System.nanoTime - t1)/1e6 + "ms")
  }
}
