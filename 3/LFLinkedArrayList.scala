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
        while (tailIndex < size && tailNode.data.get(tailIndex) != null) {
          tailNode.tail.compareAndSet(tailIndex, tailIndex+1)
          tailIndex = tailNode.tail.get
        }

        }
      } else {
        // tail node is full.
        // create new node.
        val node = new Node
        node.data.set(node.tail.getAndIncrement, x)

        if (tailNode.next.compareAndSet(null, node)) {
          done = true
          tail.compareAndSet(tailNode, node)
          // println("New Node " + x)
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
      var tailIndex = headNode.tail.get

      if (headIndex == tailIndex && next == null) {
        return None
      }
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
