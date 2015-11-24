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

import scala.reflect.ClassTag
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicReferenceArray}

/**
 * Lock-free linked arrary list based queue is implemented with the following
 * ideas:
 * - enqueueIndex points to the next slot to insert or equals to size.
 * - dequeueIndex points to the next element to dequeue.
 * - enqueue operation guarantees that when its call returns, enqueueIndex is
 *   updated until enqueueIndex points to null or equals to size.
 * - dequeue operation use CAS operation to first claim a dequeueIndex, then 
 *   return the corresponding element.
 */

class LFLinkedArrayList [T: ClassTag] extends ox.cads.collection.Queue[T] {
  private class Node{
    val enqueueIndex = new AtomicInteger(0)
    val dequeueIndex = new AtomicInteger(0)

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
      var enqueueIndex = tailNode.enqueueIndex.get
      if (enqueueIndex < size) {
        // Looks like there is an empty slot.
        while (!done && enqueueIndex < size) {
          // try to set value to the slot if null is at enqueueIndex.
          if (tailNode.data.compareAndSet(enqueueIndex, null.asInstanceOf[T], x))
            done = true
          // Invariant: enqueueIndex <= size.
          // Either the successful thread updates the enqueueIndex itself until 
          // enqueueIndex points to null or mark the node as full by setting it
          // to size or the thread failed to enqueue its element updates the 
          // enqueueIndex for the sucessful thread (which could be descheduled).
          //
          // This guarantees that when a call to enqueue returns, the element 
          // inserted could be retrieved by subsequent dequeue operations as 
          // both the element itself and the enqueueIndex are updated.
          //
          // In addition, the operation will proceed correctly if a thread is
          // descheduled after enqueuing its element as other enqueuing threads
          // would update enqueueIndex for it.
          while (enqueueIndex < size && tailNode.data.get(enqueueIndex) != null) {
            tailNode.enqueueIndex.compareAndSet(enqueueIndex, enqueueIndex+1)
            enqueueIndex = tailNode.enqueueIndex.get
          }
        }
      } else {
        // tail node is full.
        // create new node locally to the thread and compete to set the next 
        // field of the tail node to this new node.
        val node = new Node
        node.data.set(node.enqueueIndex.getAndIncrement, x)

        // The only two places where we update tail is either in the successful 
        // thread or in the failed thread. Successful thread attempts to update 
        // tail: 1) either other threads don't intervene, in which case the CAS
        // on tail finishes successfully 2) other threads intervened and updated
        // tail field for the successful thread. 
        // This allows successful thread to be descheduled after the CAS on 
        // tailNode.next field. Hence descheduling of one thread won't block
        // other threads.
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
      val dequeueIndex = headNode.dequeueIndex.get
      var enqueueIndex = headNode.enqueueIndex.get

      // enqueue operation guarantees that enqueueIndex is updated to 
      // point to null after its return. Hence, if enqueue operation hasn't
      // finished updating enqueueIndex (descheduled after enqueuing the 
      // element) returning None would still be linearizable as the 
      // enqueue/dequeue operations are concurrent.
      // Otherwise, we have the guarantee that the enqueueIndex is up-to-date,
      // hence the following condition guarantees that there is nothing to 
      // return.
      if (dequeueIndex == enqueueIndex && next == null) {
        return None
      }
      // head node is not empty or head node is empty but head.next != null.
      assert(enqueueIndex <= size)
      if (dequeueIndex < enqueueIndex) {
        // Compete to claim the dequeueIndex first, then return the 
        // corresponding element.
        if (headNode.dequeueIndex.compareAndSet(dequeueIndex, dequeueIndex+1))
          return Some(headNode.data.get(dequeueIndex))
      } else if (dequeueIndex == enqueueIndex && dequeueIndex == size) {
        // This node is used up, move to next. 
        // If it succeed, return to retry, if not, some other threads
        // have updated head so we retry as well.
        head.compareAndSet(headNode, next)
      } 
    }
    assert(false)
    None
  }
}  //  class LFLinkedArrayList


// Linearizability test PASSED for lock-based and lock-free
// queue passed for:
// 
// 1k repetitions (4 threads):
// [Lock-based queue] time taken: 2041.095ms
// [Lock-free queue] time taken: 875.582ms
//
// 10k repetitions (4 threads):
// [Lock-based queue] time taken: 9456.371ms
// [Lock-free queue] time taken: 6820.488ms
// 
// 100k repetitions (4 threads):
// [Lock-based queue] time taken: 58717.343ms
// [Lock-free queue] time taken: 56612.762ms
//
// 1 million repetitions (4 threads):
// [Lock-based queue] time taken: 573555.899ms
// [Lock-free queue] time taken: 568426.355ms
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
        val x = random.nextInt(100000)
        tester.log(me, _.enqueue(x), "enqueue("+x+")", seqEnqueue(x))
      }
      else tester.log(me, _.dequeue, "dequeue", seqDequeue)
    }
  }

  def main(args: Array[String]) {
    val reps = args(0).toInt

    val t0 = System.nanoTime
    for (i <- 0 until reps) {
      val concQueue = new LC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-based queue] time taken: " + (System.nanoTime - t0)/1e6 + "ms")

    val t1 = System.nanoTime
    for (i <- 0 until reps) {
      val concQueue = new LFC
      val seqQueue = Queue[Int]()
      val tester = new DFSLinearizabilityTester(seqQueue, concQueue, 4, worker _, 800)
      assert (tester() > 0)
    }
    println("[Lock-free queue] time taken: " + (System.nanoTime - t1)/1e6 + "ms")
  }
}


/**
 * A much easier to analyse version based on AtomicPair.
 */
import scala.reflect.ClassTag
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicReferenceArray}
import ox.cads.atomic.AtomicPair

/**
 * Lock-free linked arrary list based queue is implemented with
 * AtomicPair, which makes analysis much easier.
 */

class LFAPLinkedArrayList [T: ClassTag] extends ox.cads.collection.Queue[T] {
  private class Node {
    val data = new AtomicReferenceArray[T](size)
    var next = new AtomicReference[Node](null)
  }

  private val size = 10
  private val init = new Node
  private var head = new AtomicPair[Node, Int](init, 0)
  private var tail = new AtomicPair[Node, Int](init, 0)

  // Enqueue x
  def enqueue(x: T) = {
    var done = false
    while (!done) {
      var (tailNode, enqIdx) = tail.get
      if (enqIdx < size) {
        // Looks like there is an empty slot.
        while (!done && enqIdx < size) {
          // try to set value to the slot if null is at enqIdx.
          // LINEARIZATION Point.
          if (tailNode.data.compareAndSet(enqIdx, null.asInstanceOf[T], x))
            done = true
          tail.compareAndSet((tailNode, enqIdx), (tailNode, enqIdx+1))
          // update tail and enqIdx
          val (t, e) = tail.get
          tailNode = t; enqIdx = e
        }
      } else {
          // tail node is full.
          val node = new Node
          tailNode.next.compareAndSet(null, node)
          tail.compareAndSet((tailNode, enqIdx), (tailNode.next.get, 0))
      }
    }
  }

  /** Dequeue a value
   * @return |Some(x)| where |x| is the value dequeued, or None if empty.
   */
  def dequeue : Option[T] = {
    while (true) {
      val (headNode, deqIdx) = head.get
      val (tailNode, enqIdx) = tail.get

      if (headNode == tailNode && deqIdx == enqIdx) {
        return None
      }
      // head node is not empty or head node != tail node.
      if (headNode != tailNode && deqIdx == size) {
        head.compareAndSet((headNode, deqIdx), (headNode.next.get, 0))
      } else {
        // LINEARIZATION Point.
        if (head.compareAndSet((headNode, deqIdx), (headNode, deqIdx+1)))
          return Some(headNode.data.get(deqIdx))
      }
    }
    assert(false)
    None
  }
}  //  class LFLinkedArrayList
