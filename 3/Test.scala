import scala.collection.immutable.Queue
import ox.cads.util.ThreadUtil
import ox.cads.testing.{LinTester, DFSLinearizabilityTester}
import scala.util.Random

object Test {
  type C = ox.cads.collection.Queue[Int]
  type LC = LinkedArrayList[Int]
  type LFC = LFLinkedArrayList[Int]
  type LFAPC = LFAPLinkedArrayList[Int]
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
        val x = random.nextInt(100000)
        tester.log(me, _.enqueue(x), "enqueue("+x+")", seqEnqueue(x))
      }
      else tester.log(me, _.dequeue, "dequeue", seqDequeue)
    }
  }

  def test(q: C, reps: Int) {
    def worker = {
      val random = new scala.util.Random
      for (i <- 0 until reps) {
        if (random.nextFloat <= 0.3) {
          val x = random.nextInt(100000)
          q.enqueue(x)
        } else {
          q.dequeue
        }
      }
    }
    ox.cads.util.ThreadUtil.runSystem(4, worker)
  }

  def main(args: Array[String]) {
    val reps = args(0).toInt

    val t4 = System.nanoTime
    val concLCQueue = new LC
    test(concLCQueue, reps)
    println("[Lock-based queue] time taken: " + (System.nanoTime - t4)/1e6 + "ms")

    val t5 = System.nanoTime
    val concLFCQueue = new LFC
    test(concLFCQueue, reps)
    println("[Lock-free queue] time taken: " + (System.nanoTime - t5)/1e6 + "ms")

    val t6 = System.nanoTime
    val concLFAPQueue = new LFAPC
    test(concLFAPQueue, reps)
    println("[Lock-free AP queue] time taken: " + (System.nanoTime - t6)/1e6 + "ms")

    val t0 = System.nanoTime
    for (i <- 0 until reps) {
      val concQueue = new LC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-based queue] test time taken: " + (System.nanoTime - t0)/1e6 + "ms")

    val t3 = System.nanoTime
    for (i <- 0 until reps) {
      val concQueue = new LFAPC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[AP Lock-free queue] test time taken: " + (System.nanoTime - t3)/1e6 + "ms")

    val t1 = System.nanoTime
    for (i <- 0 until reps) {
      val concQueue = new LFC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-free queue] test time taken: " + (System.nanoTime - t1)/1e6 + "ms")
  }
}
