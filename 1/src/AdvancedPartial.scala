// A more efficient implementaiton of partial solutions

class AdvancedPartial extends SimplePartial{
  // Inherited from SimplePartial
  // private val contents = Array.ofDim[Int](9,9)
  // Abs: forall i, j 
  //        (contents(i)(j)=0 <=> (i,j) not in dom board) && 
  //        (contents(i)(j)>0 <=> contents(i)(j)=board(i,j))
  // The abstract DTI is implicitly inherited. 

  // pos(i)(j) is a list of all values that can be placed in square (i,j)
  private val pos = Array.ofDim[List[Int]](9,9)
  // DTI: pos(i)(j) = { d | board (+) {(i,j) -> d} satisfies the abstract DTI }

  // Initialise from a file
  override def init(fname:String) = {
    // Initialise all pos lists to hold all values
    for(i <- 0 until 9; j <- 0 until 9) 
      pos(i)(j) = (1 to 9).toList

    val lines = scala.io.Source.fromFile(fname).getLines
    for(i <- 0 until 9){
      val line = lines.next
      for(j <- 0 until 9){
    	val c = line.charAt(j)
    	if(c.isDigit) makePlay(i, j, c.asDigit)
    	else { assert(c=='.'); contents(i)(j) = 0 }
      }
    }
  }

  // Play d in position (i,j), remove d from the rest of this row, column and
  // block.  In terms of the abstraction:
  // Pre: board (+) {(i,j) -> d} satisfies the abstract DTI
  // Post: board = board_0 (+) {(i,j) -> d}
  private def makePlay(i:Int,j:Int,d:Int) = {
    contents(i)(j) = d; pos(i)(j) = List(d)
    // Remove d from row i
    for(j1 <- 0 until 9; if j1!=j) pos(i)(j1) = pos(i)(j1).filter(_ != d)
    // Remove d from column j
    for(i1 <- 0 until 9; if i1!=i) pos(i1)(j) = pos(i1)(j).filter(_ != d)
    // Remove d from this 3x3 block
    val basei = i/3*3; val basej = j/3*3
    for(i1 <- basei until basei+3; j1 <- basej until basej+3) 
      pos(i1)(j1) = pos(i1)(j1).filter(_ != d)
  }

  // Print -- as in SimplePartial
  // def printPartial 

  // Is the partial solution complete? -- as in SimplePartial
  // def complete : Boolean 

  // Find a blank position to play in; precondition: complete returns false.
  // We find the square that gives the best score, as defined below
  override def nextPos : (Int,Int) = {
    var bestScore = 0; var bestPos = (0,0)
    for(i <- 0 until 9; j <- 0 until 9){
      val thisScore = score(i,j)
      if(thisScore > bestScore){ bestPos = (i,j); bestScore = thisScore }
    }
    assert(bestScore>0)
    bestPos
  }

  // Return measure of how good it would be to play in position (i,j); the
  // result will be greater than 0 if and only if this square has not yet been
  // played in
  private def score(i:Int, j:Int) : Int = {
    if(contents(i)(j) != 0) 0
    else 10-pos(i)(j).length
    // else{
    //   var s = 28*9+1 - pos(i)(j).length;
    //   for(i1 <- 0 until 9) s -= pos(i1)(j).length;
    //   for(j1 <- 0 until 9) s -= pos(i)(j1).length;
    //   val basei = i/3*3; val basej = j/3*3;
    //   for(i1 <- basei until basei+3; j1 <- basej until basej+3) 
    // 	s -= pos(i1)(j1).length;
    //   assert(s>0); return s;
    // }
  }

  // Can we play value d in position (i,j); precondition: (i,j) is blank
  override def canPlay(i:Int, j:Int, d:Int) : Boolean = pos(i)(j).contains(d)

  // Create a new partial solution, extending this one by playing d in
  // position (i,j)
  override def play(i:Int, j:Int, d:Int) : Partial = {
    // Make a copy of this in p
    val p = new AdvancedPartial
    for(i1 <- 0 until 9; j1 <- 0 until 9){
      p.contents(i1)(j1) = contents(i1)(j1)
      p.pos(i1)(j1) = pos(i1)(j1)
    }
    // Now play d in (i,j)
    p.makePlay(i,j,d); p
  }
}
