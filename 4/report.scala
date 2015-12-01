/**
 * Tester
 *
 * msc16[/ox-concurrent-algorithms-and-datastructures/4]$ scala Test 10000 4
 * [Shared Freelist Lock-free stack] test time taken: 7179.021835ms
 * [Shared Freelist Lock-free stack] created: 222223 recycled: 2166091 (0.90695405)
 * [ThreadLocal Lock-free ABA stack] test time taken: 14812.089337ms
 * [ThreadLocal Lock-free ABA stack] created: 231401 recycled: 2150622 (0.9028553)
 */
import scala.collection.immutable.Stack
import ox.cads.util.ThreadUtil
import ox.cads.testing.{LinTester, DFSLinearizabilityTester}
import scala.util.Random

object Test {
  type C = ox.cads.collection.TotalStack[Int]
  type SLFS =  StampedLockFreeStack[Int]
  type SFLFS =  SharedFreeListStack[Int]
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
      val conc= new SFLFS(threads)

      val seq= Stack[Int]()
      val tester = new DFSLinearizabilityTester(seq, conc, threads, worker _, 800)
      assert (tester() > 0)
      val (re, cr) = conc.recycleRate
      r += re; c += cr
    }
    println("[Shared Freelist Lock-free stack] test time taken: " + (System.nanoTime - t0)/1e6 + "ms")
    println("[Shared Freelist Lock-free stack] created: " + c + " recycled: " + r + " (" + r.toFloat/(r+c)+ ")")

    val t1 = System.nanoTime
    r = 0; c = 0
    for (i <- 0 until reps) {
      val conc= new SLFS

      val seq= Stack[Int]()
      val tester = new DFSLinearizabilityTester(seq, conc, threads, worker _, 800)
      assert (tester() > 0)
      val (re, cr) = conc.recycleRate
      r += re; c += cr
    }
    println("[ThreadLocal Lock-free ABA stack] test time taken: " + (System.nanoTime - t0)/1e6 + "ms")
    println("[ThreadLocal Lock-free ABA stack] created: " + c + " recycled: " + r + " (" + r.toFloat/(r+c)+ ")")
  }
}
/**
 * Shared Pool.
 */
import ox.cads.atomic.AtomicPair
import ox.cads.collection.TotalStack
import ox.cads.util.ThreadID
import scala.util.Random

// Shared free list stack use a shared common pool by all threads 
// with each thread recycling in its own linked list. 
// Each thread access and store recycled nodes in list[p] where p
// is its threadID.
//
// Every next reference is associated with a stamp that is incremental
// on each update. Essentially, the same stamp is never reused even when
// recycled. This guard against the ABA problem. 
//
// The test script tests this specifically by setting the input range
// narrow: 0..5.
class SharedFreeListStack[T](p : Int) extends TotalStack[T] {
  private class Node(v : T) {
    var stampedNext = new AtomicPair[Node, Int](null, 0)
    var value: T = v
  }

  private val top = new AtomicPair[Node, Int](null, 0)
  private val freeList = Array.fill(p)(null : Node)
  private val random = new scala.util.Random
  
  private var recycled = 0
  private var created = 0

  private def pause = ox.cads.util.NanoSpin(random.nextInt(500))

  private def recycleOrCreate(value : T) : Node = {
    val id = ThreadID.get % p
    if (freeList(id) == null) {
      created += 1
      new Node(value)
    } else {
      recycled += 1
      val n = freeList(id)
      freeList(id) = n.stampedNext.getFirst
      n.value = value
      n
    }
  }

  def recycleRate: (Int, Int) = (recycled, created)

  /** Push value onto the stack */
  def push(value: T) = {
    val node = recycleOrCreate(value)
    var done = false
    do {
      val (oldTop, stamp) = top.get
      val oldNextStamp = node.stampedNext.getSecond
      node.stampedNext.set(oldTop, oldNextStamp+1)
      done = top.compareAndSet((oldTop, stamp), (node, stamp+1))
      if (!done) pause // back off
    } while (!done)
  }

  /** Pop a value from the stack. Return None if the stack is empty. */
  def pop : Option[T] = {
    var result : Option[T] = None; var done = false
    do {
      val (oldTop, stamp) = top.get
      if (oldTop == null) done = true // empty stack; return None
      else {
        val newTop = oldTop.stampedNext.getFirst
        // try to remove oldTop from list
        if(top.compareAndSet((oldTop, stamp), (newTop, stamp+1))) {
          result = Some(oldTop.value)
          done = true
          
          // Recycle oldTop.
          val id = ThreadID.get % p
          val firstFreeNode = freeList(id)
          oldTop.stampedNext.set(firstFreeNode, stamp+1)
          freeList(id) = oldTop
        }
        else pause
      }
    } while(!done)
    result
  }
}

/**
 * ThreadLocal
 */
import ox.cads.atomic.AtomicPair
import ox.cads.collection.TotalStack
import scala.util.Random

// StampedLockFreeStack uses a thread local pool by each thread 
// with each thread recycling in its own thread local linked list. 
//
// Every next reference is associated with a stamp that is incremental
// on each update. Essentially, the same stamp is never reused even when
// recycled. This guard against the ABA problem. 
//
// The test script tests this specifically by setting the input range
// narrow: 0..5.
class StampedLockFreeStack[T] extends TotalStack[T] {
  private class Node(v : T) {
    var stampedNext = new AtomicPair[Node, Int](null, 0)
    var value: T = v
  }

  private val top = new AtomicPair[Node, Int](null, 0)
  private val freeList = new ThreadLocal[Node]
  private val random = new scala.util.Random
  
  private var recycled = 0
  private var created = 0

  private def pause = ox.cads.util.NanoSpin(random.nextInt(500))

  private def recycleOrCreate(value : T) : Node = {
    if (freeList.get == null) {
      created += 1
      new Node(value)
    } else {
      recycled += 1
      val n = freeList.get
      freeList.set(n.stampedNext.getFirst)
      n.value = value
      n
    }
  }

  private def recycle(n : Node, stamp : Int) = {
    val firstFreeNode = freeList.get
    n.stampedNext.set(firstFreeNode, stamp+1)
    freeList.set(n)
  }

  def recycleRate: (Int, Int) = (recycled, created)

  /** Push value onto the stack */
  def push(value: T) = {
    val node = recycleOrCreate(value)
    var done = false
    do {
      val (oldTop, stamp) = top.get
      val oldNextStamp = node.stampedNext.getSecond
      node.stampedNext.set(oldTop, oldNextStamp+1)
      done = top.compareAndSet((oldTop, stamp), (node, stamp+1))
      if (!done) pause // back off
    } while (!done)
  }

  /** Pop a value from the stack. Return None if the stack is empty. */
  def pop : Option[T] = {
    var result : Option[T] = None; var done = false
    do {
      val (oldTop, stamp) = top.get
      if (oldTop == null) done = true // empty stack; return None
      else {
        val newTop = oldTop.stampedNext.getFirst
        // try to remove oldTop from list
        if(top.compareAndSet((oldTop, stamp), (newTop, stamp+1))) {
          result = Some(oldTop.value)
          done = true

          // Recycle oldTop.
          recycle(oldTop, stamp)
        }
        else pause
      }
    } while(!done)
    result
  }
}
