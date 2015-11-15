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
