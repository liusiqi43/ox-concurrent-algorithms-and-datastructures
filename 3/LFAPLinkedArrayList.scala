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
