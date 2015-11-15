import ox.cads.collection.Pool
import ox.cads.collection.LockFreeStack
import ox.cads.util.Profiler

class LockFreeStackPool[T] extends Pool[T] {
  private var stack = new LockFreeStack[T]

  /** put x into the pool */
  def add(x: T) : Unit = {
    Profiler.count("stack-push")
    stack.push(x)
  }

  /** Get a value from the pool.
    * @return Some(x) where x is the value obtained, or None if the pool is
    * empty. */
  def get : Option[T] = {
    stack.pop
  }
}
