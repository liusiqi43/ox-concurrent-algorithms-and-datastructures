import scala.reflect.ClassTag
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicReferenceArray}
import ox.cads.atomic.AtomicPair

/**
 * Lock-free linked arrary list based queue is implemented with the following
 * ideas:
 * - enqIdx points to the next slot to insert or equals to size.
 * - deqIdx points to the next element to dequeue.
 * - enqueue operation guarantees that when its call returns, enqIdx is
 *   updated until enqIdx points to null or equals to size.
 * - dequeue operation use CAS operation to first claim a deqIdx, then 
 *   return the corresponding element.
 */

class LFAPLinkedArrayList [T: ClassTag] extends ox.cads.collection.Queue[T] {
  private class Node{
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

      // enqueue operation guarantees that enqIdx is updated to 
      // point to null after its return. Hence, if enqueue operation hasn't
      // finished updating enqIdx (descheduled after enqueuing the 
      // element) returning None would still be linearizable as the 
      // enqueue/dequeue operations are concurrent.
      // Otherwise, we have the guarantee that the enqIdx is up-to-date,
      // hence the following condition guarantees that there is nothing to 
      // return.
      if (headNode == tailNode && deqIdx == enqIdx) {
        return None
      }
      // head node is not empty or head node != tail node.
      if (headNode != tailNode && deqIdx == size) {
        // This node is used up, move to next. 
        // If it succeed, return to retry, if not, some other threads
        // have updated head so we retry as well.
        head.compareAndSet((headNode, deqIdx), (headNode.next.get, 0))
      } else {
        // Compete to claim the deqIdx first, then return the 
        // corresponding element.
        if (head.compareAndSet((headNode, deqIdx), (headNode, deqIdx+1)))
          return Some(headNode.data.get(deqIdx))
      }
    }
    assert(false)
    None
  }
}  //  class LFLinkedArrayList
