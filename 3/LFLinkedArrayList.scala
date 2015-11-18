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
