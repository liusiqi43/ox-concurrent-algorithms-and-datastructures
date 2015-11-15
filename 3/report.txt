// Concurrent Algorithms and Data Structures - Praticals 3
// Siqi Liu (MSc)

// Lock-based LinkedArrayList
import scala.reflect.ClassTag
import java.util.concurrent.locks.ReentrantLock

class LinkedArrayList [T: ClassTag] extends ox.cads.collection.Queue[T] {
  private class Node{
    var head = 0
    var tail = 0

    val data = new Array[T](size)
    // made volatile to prevent reordering.
    var next : Node = null
  }

  private var head : Node = null
  private var tail : Node = null
  private val size = 10
  private val enqueue_lock = new ReentrantLock()
  private val dequeue_lock = new ReentrantLock()

  // Enqueue x
  def enqueue(x: T) = {
    enqueue_lock.lock
    try {
      if (tail != null && tail.tail < size) {
        tail.data(tail.tail) = x
        tail.tail += 1
      } else {
        // tail node is full or is null, append a new node to tail.
        val node = new Node
        node.data(node.tail) = x
        node.tail += 1
        if (tail != null) {
          tail.next = node
          tail = tail.next
        } else {
          tail = node
          head = node
        }
      }
    } finally {
      enqueue_lock.unlock
    }
  }

  /** Dequeue a value
   * @return |Some(x)| where |x| is the value dequeued, or None if empty.
   */
  def dequeue : Option[T] = {
    dequeue_lock.lock
    try {
      if (head == null || (head.head == head.tail && head.next == null)) return None
      // head is not null and if head node is empty and head.next != null, then head
      // node must have used all slots, move to the next node.
      if (head.head == head.tail && head.head == size) head = head.next
      // Next must contain at least one element ready for dequeuing.
      val output = head.data(head.head)
      head.head += 1
      Some(output)
    } finally {
      dequeue_lock.unlock
    }
  }
}

// Lock-free LinkedArrayList
import scala.reflect.ClassTag
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicReferenceArray}

class LFLinkedArrayList [T: ClassTag] extends ox.cads.collection.Queue[T] {
  private class Node{
    val head = new AtomicInteger(0)
    val tail = new AtomicInteger(0)

    val data = new AtomicReferenceArray[T](size)
    var next = new AtomicReference[Node](null)
  }

  private val size = 10
  private val init = new Node
  private var head = new AtomicReference[Node](init)
  private var tail = new AtomicReference[Node](init)

  // Enqueue x
  def enqueue(x: T) = {
    var done = false
    while (!done) {
      var tailNode = tail.get
      var tailIndex = tailNode.tail.get
      if (tailIndex < size) {
        // Looks like there is an empty slot.
        while (!done && tailIndex < size) {
          // try to set value to the slot.
          if (tailNode.data.compareAndSet(tailIndex, null.asInstanceOf[T], x)) {
            done = true
          }
          // tailIndex == size --> node is full
          // Invariant: tailIndex <= size
          tailNode.tail.compareAndSet(tailIndex, tailIndex+1)
          tailIndex = tailNode.tail.get
        }
      } else {
        // tail node is full.
        // create new node.
        val node = new Node
        node.data.set(node.tail.getAndIncrement, x)

        if (tailNode.next.compareAndSet(null, node)) {
          done = true
          tail.compareAndSet(tailNode, node)
        } else {
          var tailNode = tail.get
          var next = tailNode.next.get
          while (next != null) {
            tail.compareAndSet(tailNode, next)
            tailNode = tail.get
            next = tailNode.next.get
          }
        }
      }
    }
  }

  /** Dequeue a value
   * @return |Some(x)| where |x| is the value dequeued, or None if empty.
   */
  def dequeue : Option[T] = {
    while (true) {
      val headNode = head.get
      val next = headNode.next.get
      val headIndex = headNode.head.get
      val tailIndex = headNode.tail.get
      if (headIndex == tailIndex && next == null)
        return None

      // head node is not empty or head node is empty but head.next != null.
      assert(tailIndex <= size)
      if (headIndex < tailIndex) {
        if (headNode.head.compareAndSet(headIndex, headIndex+1))
          return Some(headNode.data.get(headIndex))
      } else if (headIndex == tailIndex && headIndex == size) {
        // This node is used up, move to next. 
        // If it succeed, return to retry, if not, some other threads
        // have updated head so we retry as well.
        head.compareAndSet(headNode, next)
      } else {
        assert(false)
      }
    }
    assert(false)
    None
  }
}  //  class LFLinkedArrayList

// Linearizability test: PASSED.
// For 10k repetitions (4 threads):
// [Lock-based queue] time taken: 9456.371ms
// [Lock-free queue] time taken: 6820.488ms
// 
// For 1k repetitions (4 threads):
// [Lock-based queue] time taken: 1999.154ms
// [Lock-free queue] time taken: 772.846ms
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
    for (i <- 0 until 10000) {
      val concQueue = new LC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-based queue] time taken: " + (System.nanoTime - t0)/1e6 + "ms")

    val t1 = System.nanoTime
    for (i <- 0 until 10000) {
      val concQueue = new LFC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-free queue] time taken: " + (System.nanoTime - t1)/1e6 + "ms")
  }
}
