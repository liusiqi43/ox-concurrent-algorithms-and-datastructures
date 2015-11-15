/**
 * Sudoku solver.
 */

 import scala.collection.mutable.ArrayBuffer
 import ox.cads.util.Profiler
 import java.util.concurrent.atomic.AtomicBoolean

 object Sudoku {
  /* Solve the puzzle defined by init sequentially 

    scala Sudoku -p 8 --all -n 100
    Time taken: 15282

  */
  def sSolve(init: Partial) {
    // Stack to store partial solutions that we might back-track to.
    val stack = new scala.collection.mutable.Stack[Partial]
    stack.push(init)
    var done = false

    while (!stack.isEmpty && !done) {
      val partial = stack.pop
      if (partial.complete) { // done!
        partial.printPartial; done = true
        } else {
        // Choose position to play
        val (i, j) = partial.nextPos;
        // Consider all values to play there
        for (d <- 1 to 9)
        if (partial.canPlay(i, j, d)) {
          val p1 = partial.play(i, j, d); stack.push(p1)
        }
      }
    }
  }

  /* Solve the puzzle defined by init in naive parallel 

    Stuck on the impossible.sud case as the stack would be exhausted and all threads
    will be spinning on the empty stack while done = false.
  */
  def pSolve(init: Partial, par: Int) {
    val stack = new ox.cads.collection.LockFreeStack[Partial]
    val done = new AtomicBoolean(false)
    var partial = None

    // Stack to store partial solutions that we might back-track to.
    stack.push(init)

    def solve() {
      while (!done.get) {
        stack.pop match {
          case None => {}
          case Some(partial) => {
            val complete = partial.complete
            if (complete && done.compareAndSet(false, true)) {
              partial.printPartial;
              } else if (!complete) {
              // Choose position to play
              val (i, j) = partial.nextPos;
              // Consider all values to play there
              for (d <- 1 to 9)
              if (partial.canPlay(i, j, d)) {
                val p1 = partial.play(i, j, d)
                if (!done.get) stack.push(p1)
              }
            }
          }
        }
      }
    }

    ox.cads.util.ThreadUtil.runSystem(par, solve)
  }

  /* Solve the puzzle defined by init in parallel with termination detecting pool 

    scala Sudoku -p 16 --pool --all -n 100
    Time taken: 26877
    COUNTERS:
    stack-push: 17847111

    scala Sudoku -p 8 --pool --all -n 100
    impossible.sud
    Time taken: 8913
    COUNTERS:
    stack-push: 18629985

  */
  def tSolve(init: Partial, par: Int) {
    val pool = new ox.cads.collection.TerminationDetectingPool[Partial](new LockFreeStackPool[Partial], par)
    val done = new AtomicBoolean(false)

    // Stack to store partial solutions that we might back-track to.
    pool.add(init)

    def solve() {
      while (!done.get) {
        // pool.get will spin until pool is not empty or some thread invoke signalDone.
        pool.get match {
          // Return immediate upon one unsuccessful get.
          case None => { return }
          case Some(partial) => {
            val complete = partial.complete
            if (complete && done.compareAndSet(false, true)) {
              pool.signalDone
              partial.printPartial;
              } else if (!complete) {
              // Choose position to play
              val (i, j) = partial.nextPos;
              // Consider all values to play there
              for (d <- 1 to 9)
              if (partial.canPlay(i, j, d)) {
                val p1 = partial.play(i, j, d)
                pool.add(p1)
              }
            }
          }
        }
      }
    }

    ox.cads.util.ThreadUtil.runSystem(par, solve)
  }

  /* Solve the puzzle defined by init while avoiding excessive stack-push 

    We avoid unnecessary stack-push by immediately taking the next step if there 
    is no ambiguity. We only push partials to stack if multiple possibilities exist
    regarding the next step to take. This saves about 1/3 of the total stack-push.

    Alternatively, we could also create local stack within each thread to balance the
    load on the global stack.

    scala Sudoku -p 16 --opt --all -n 100
    Time taken: 25303
    COUNTERS:
    stack-push:       11765240
    unambiguous-step: 5862303

    scala Sudoku -p 8 --opt --all -n 100
    Time taken: 7653
    COUNTERS:
    stack-push:       11968332
    unambiguous-step: 5894634

    scala Sudoku -p 7 --opt --all -n 100
    Time taken: 6526
    COUNTERS:
    stack-push:       12244262
    unambiguous-step: 6065317

    scala Sudoku -p 6 --opt --all -n 100
    Time taken: 6787
    COUNTERS:
    stack-push:       12524009
    unambiguous-step: 6229913

    scala Sudoku -p 5 --opt --all -n 100
    Time taken: 6907
    COUNTERS:
    stack-push:       12617463
    unambiguous-step: 6221822

    scala Sudoku -p 4 --opt --all -n 100
    Time taken: 7011
    COUNTERS:
    stack-push:       12347943
    unambiguous-step: 6082012

    scala Sudoku -p 2 --opt --all -n 100
    Time taken: 9980
    COUNTERS:
    stack-push:       12444506
    unambiguous-step: 6112263

  */
  def oSolve(init: Partial, par: Int) {
    val pool = new ox.cads.collection.TerminationDetectingPool[Partial](new LockFreeStackPool[Partial], par)
    val done = new AtomicBoolean(false)

    // Stack to store partial solutions that we might back-track to.
    pool.add(init)

    def solve() {
      while (!done.get) {
        pool.get match {
          case None => { return }
          case Some(p) => {
            val actions = new ArrayBuffer[(Int, Int, Int)]
            var unambiguous = false
            var partial = p
            do {
              actions.clear
              unambiguous = false
              val complete = partial.complete
              if (complete && done.compareAndSet(false, true)) {
                pool.signalDone
                partial.printPartial;
                return
                } else if (!complete) {
                  val (i, j) = partial.nextPos;
                // Choose position to play
                for (d <- 1 to 9)
                if (partial.canPlay(i, j, d)) actions += ((i, j, d))

                unambiguous = (actions.length == 1) 
                if (unambiguous) {
                  Profiler.count("unambiguous-step")
                  partial = partial.play(actions(0)._1, actions(0)._2, actions(0)._3)
                }
              }    
              } while (unambiguous)

              for (d <- actions) {
                val p1 = partial.play(d._1, d._2, d._3)
                pool.add(p1)
              }
            }
          }
        }
      }

      ox.cads.util.ThreadUtil.runSystem(par, solve)
    }

  /** A list of files containing possible puzzles */
  private val allPossibleFiles =
  List("test1.sud", "test2.sud", "test3.sud", "test4.sud", "test5.sud",
    "test6.sud", "test7.sud", "test8.sud", "test9.sud", "test10.sud")
  /** A list of files containing puzzles, including one impossible one. */
  private val allFiles = allPossibleFiles ++ List("impossible.sud")

  def main(args: Array[String]) = {
    val t0 = System.currentTimeMillis()

    // options
    var count = 1 // number of tests
    var par = 1 // number of parallelism 
    var fname = "" // filename
    var adv = false // are we using the AdvancedPartial?
    var stack = false // use the stack implementation for naive parallelism
    var pool = false   // use the TerminationDetectingPool.
    var opt = false   // use optimized strategy to avoid excessive stack pushing. 
    var all = false // are we doing all files in allFiles?
    var allPoss = false // are we doing all files in allPossibleFiles?
    // parse command line arguments
    var i = 0
    while (i < args.length) {
      if (args(i) == "-n") { count = args(i + 1).toInt; i += 2 }
      else if (args(i) == "-p") { par = args(i + 1).toInt; i += 2 }
      else if (args(i) == "-a") { adv = true; i += 1 }
      else if (args(i) == "--all") { all = true; i += 1 }
      else if (args(i) == "--allPoss") { allPoss = true; i += 1 }
      else if (args(i) == "--stack") { stack = true; i += 1 }
      else if (args(i) == "--pool") { pool = true; i += 1 }
      else if (args(i) == "--opt") { opt = true; i += 1 }
      else { fname = args(i); i += 1 }
    }
    assert(all || allPoss || fname != "")

    // Initialise partial from file fname
    def mkPartial(fname: String) = {
      val partial = if (adv) new AdvancedPartial else new SimplePartial
      partial.init(fname)
      partial
    }

    // Solve count times
    if (stack) {
      for (i <- 0 until count) {
        if (all) for (f <- allFiles) { println(f); pSolve(mkPartial(f), par) }
        else if (allPoss)
        for (f <- allPossibleFiles) { println(f); pSolve(mkPartial(f), par) }
        else pSolve(mkPartial(fname), par)
      }
      } else if (pool) {
        for (i <- 0 until count)
        if (all) for (f <- allFiles) { println(f); tSolve(mkPartial(f), par) }
        else if (allPoss)
        for (f <- allPossibleFiles) { println(f); tSolve(mkPartial(f), par) }
        else tSolve(mkPartial(fname), par)
        } else if (opt) {
          for (i <- 0 until count)
          if (all) for (f <- allFiles) { println(f); oSolve(mkPartial(f), par) }
          else if (allPoss)
          for (f <- allPossibleFiles) { println(f); oSolve(mkPartial(f), par) }
          else oSolve(mkPartial(fname), par)
          } else {
            for (i <- 0 until count)
            if (all) for (f <- allFiles) { println(f); sSolve(mkPartial(f)) }
            else if (allPoss)
            for (f <- allPossibleFiles) { println(f); sSolve(mkPartial(f)) }
            else sSolve(mkPartial(fname))
          }

          println("Time taken: " + (System.currentTimeMillis() - t0))
          Profiler.report
        }
      }

