import ox.cads.atomic.AtomicPair
import ox.cads.collection.TotalStack
import scala.util.Random

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
    freeList = n 
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
