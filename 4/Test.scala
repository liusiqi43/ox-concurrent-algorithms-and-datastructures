import scala.collection.immutable.Stack
import ox.cads.util.ThreadUtil
import ox.cads.testing.{LinTester, DFSLinearizabilityTester}
import scala.util.Random

object Test {
  type C = ox.cads.collection.TotalStack[Int]
  type SLFS =  StampedLockFreeStack[Int]
  type S = Stack[Int]
    
  def seqPush(x: Int)(stack: S) : (Unit, S) = ((), stack.push(x))
  def seqPop(stack: S) : (Option[Int], S) = {
    if (stack.isEmpty) (None, stack)
    else {val (r, stack1) = stack.pop2; (Some(r), stack1)}
  }

  def worker(me: Int , tester : LinTester[S, C]) = {
    val random = new scala.util.Random
    for (i <- 0 until 200) {
      if(random.nextFloat <= 0.3){
        val x = random.nextInt(5)
        tester.log(me, _.push(x), "push("+x+")", seqPush(x))
      }
      else tester.log(me, _.pop, "pop", seqPop)
    }
  }

  def main(args: Array[String]) {
    val reps = args(0).toInt
    val threads = args(1).toInt

    val t0 = System.nanoTime
    var (r, c) = (0, 0)
    for (i <- 0 until reps) {
      val conc= new SLFS(threads)
      val seq= Stack[Int]()
      val tester = new DFSLinearizabilityTester(seq, conc, threads, worker _, 800)
      assert (tester() > 0)
      val (re, cr) = conc.recycleRate
      r += re; c += cr
    }
    println("[Lock-free ABA stack] test time taken: " + (System.nanoTime - t0)/1e6 + "ms")
    println("[Lock-free ABA stack] created: " + c + " recycled: " + r + " (" + r.toFloat/(r+c)+ ")")
  }
}
