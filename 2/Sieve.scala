import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean

object Sieve {
  /**
   * InsertPrime tries to insert prime at last index and guarantee thread-safety
   * using CAS operation. 
   */
  def insertPrime(primes: AtomicIntegerArray, prime: Int, start: Int): Boolean = {
    var i = start
    var p = prime

    while (p != 0) {
      var cur = primes.get(i)
      while (cur > 0 && cur < p) {
        i += 1
        if (i >= primes.length) return false
        cur = primes.get(i)
      }

      if (primes.compareAndSet(i, cur, p)) {
        p = cur 
      } 
    }
    true
  }

  /**
   * Returns prime at index i. Try to read from local cache first, if failed,
   * read from shared primes array and update local cache if appropriate.
   */
  def getPrime(local: Array[Int], global: AtomicIntegerArray, i : Int, test: Int): Int = {
    var p = local(i)
    if (p == 0) {
      p = global.get(i)
      if (p * p <= test) local(i) = p
    }
    return p
  }

  /**
   * Solve concurrently without local caching.
   */
  def pSolve(N: Int, par: Int) = {
    val primes = new AtomicIntegerArray(N) 
    val current = new Array[Int](par)
    val next = new AtomicInteger(3)
    var nextSlot = new AtomicInteger(1) 
    primes.set(0, 2)

    def solve(id: Int) = {
      while (primes.get(N-1) == 0) {
        var test = next.getAndIncrement
        var start = nextSlot.get

        current(id) = test
        var valid = false
        while (!valid) {
          valid = true
          var i = 0
          for (i <- 0 until par) {
            val c = current(i)
            if (c <= Math.sqrt(test))
              valid = false
          }
        }

        var isPrime = true 
        var i = 0; var p = primes.get(i)
        while (isPrime && p > 0 && i < primes.length-1 && p <= Math.sqrt(test)) {
          if (test % p == 0)
            isPrime = false
          i += 1
          p = primes.get(i)
        }

        if (isPrime && insertPrime(primes, test, start)) {
           nextSlot.getAndIncrement
        }
      }
    }

    ox.cads.util.ThreadUtil.runIndexedSystem(par, solve)
    println(primes.get(N - 1))
  }

  /**
   * Solve concurrently with local caching.
   */
  def pLocalSolve(N: Int, par: Int) = {
    val primes = new AtomicIntegerArray(N) 
    val current = new Array[Int](par)
    val next = new AtomicInteger(3)
    var nextSlot = new AtomicInteger(1) 
    primes.set(0, 2)

    def solve(id: Int) = {
      val localPrimes = new Array[Int](Math.sqrt(N).toInt)
      while (primes.get(N-1) == 0) {
        var test = next.getAndIncrement
        var start = nextSlot.get

        current(id) = test
        var valid = false
        while (!valid) {
          valid = true
          var i = 0
          for (i <- 0 until par) {
            var o = current(i).toLong
            if (o > 0 && o * o <= test) {
              valid = false
            }
          }
        }

        var isPrime = true 
        var i = 0; var p = getPrime(localPrimes, primes, i, test)
        while (isPrime && p > 0 && i < primes.length-1 && p * p <= test) {
          if (test % p == 0)
            isPrime = false
          i += 1
          p = getPrime(localPrimes, primes, i, test)
        }

        if (isPrime && insertPrime(primes, test, start)) {
           nextSlot.getAndIncrement
        }
      }
    }

    ox.cads.util.ThreadUtil.runIndexedSystem(par, solve)
    println(primes.get(N - 1))
  }


  def sSolve(N: Int) = {
    val primes = new Array[Int](N) // will hold the primes
    primes(0) = 2
    var nextSlot = 1 // next free slot in primes
    var next = 3 // next candidate prime to consider

    while (nextSlot < N) {
      // Test if next is prime; 
      // invariant: next is coprime with primes[0..i) && p = primes(i)
      var i = 0; var p = primes(i)
      while (p * p <= next && next % p != 0) { i += 1; p = primes(i) }
      if (p * p > next) { // next is prime
        primes(nextSlot) = next; nextSlot += 1
      }
      next += 1
    }

    println(primes(N - 1))
  }

  /**
   * scala -J-Xmx2g Sieve 100000 2
   * Solve with sequential implementation.
   * 1299709
   * Time taken: 93
   * Solve with concurrent implementation.
   * 1299709
   * Time taken: 656
   * Solve with localized concurrent implementation.
   * 1299709
   * Time taken: 662
   *
   * scala -J-Xmx2g Sieve 100000 4
   * Solve with sequential implementation.
   * 1299709
   * Time taken: 94
   * Solve with concurrent implementation.
   * 1299709
   * Time taken: 396
   * Solve with localized concurrent implementation.
   * 1299709
   * Time taken: 354
   *
   * scala -J-Xmx2g Sieve 100000 6
   * Solve with sequential implementation.
   * 1299709
   * Time taken: 97
   * Solve with concurrent implementation.
   * 1299709
   * Time taken: 362
   * Solve with localized concurrent implementation.
   * 1299709
   * Time taken: 368
   *
   * scala -J-Xmx2g Sieve 100000 8
   * Solve with sequential implementation.
   * 1299709
   * Time taken: 97
   * Solve with concurrent implementation.
   * 1299709
   * Time taken: 459
   * Solve with localized concurrent implementation.
   * 1299709
   * Time taken: 355
   * 
   * scala -J-Xmx2g Sieve 100000 7
   * Solve with sequential implementation.
   * 1299709
   * Time taken: 97
   * Solve with concurrent implementation.
   * 1299709
   * Time taken: 374
   * Solve with localized concurrent implementation.
   * 1299709
   * Time taken: 350
   *
   * scala -J-Xmx2g Sieve 1000000 7
   * Solve with sequential implementation.
   * 15485863
   * Time taken: 2249
   * Solve with concurrent implementation.
   * 15485863
   * Time taken: 8691
   * Solve with localized concurrent implementation.
   * 15485863
   * Time taken: 8580
   */
  def main(args: Array[String]) = {
    assert(args.length == 2, "must have one argument")
    val t0 = java.lang.System.currentTimeMillis()

    val N = args(0).toInt // number of primes required.
    val P = args(1).toInt // number of concurrent threads.
    println("Solve with sequential implementation.")
    sSolve(N)
    println("Time taken: " + (java.lang.System.currentTimeMillis() - t0))

    println("Solve with concurrent implementation.")
    val t1 = java.lang.System.currentTimeMillis()
    pSolve(N, P)
    println("Time taken: " + (java.lang.System.currentTimeMillis() - t1))

    println("Solve with localized concurrent implementation.")
    val t2 = java.lang.System.currentTimeMillis()
    pLocalSolve(N, P)
    println("Time taken: " + (java.lang.System.currentTimeMillis() - t2))
  }
}
