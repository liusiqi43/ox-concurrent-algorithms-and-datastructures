import ox.cads.util.Profiler

// A simple implementation of partial solutions
class SimplePartial extends Partial{
  protected val contents = Array.ofDim[Int](9,9)
  // Abs: forall i, j 
  //        (contents(i)(j)=0 <=> (i,j) not in dom board) && 
  //        (contents(i)(j)>0 <=> contents(i)(j)=board(i,j))
  // The abstract DTI is implicitly inherited. 

  // Initialise from a file
  def init(fname:String) = {
    val lines = scala.io.Source.fromFile(fname).getLines
    for(i <- 0 until 9){
      val line = lines.next
      for(j <- 0 until 9){
	val c = line.charAt(j)
	if(c.isDigit) contents(i)(j) = c.asDigit
	else { assert(c=='.'); contents(i)(j) = 0 }
      }
    }
  }

  // Print
  def printPartial = {
    for(i <- 0 until 9){
      for(j <- 0 until 9) print(contents(i)(j))
      println 
    }
    println
  }

  // Is the partial solution complete?
  def complete : Boolean = {
    var i = 0; var j = 0;
    while(i < 9 && contents(i)(j) != 0)
      if(j < 8) j += 1 else { j = 0; i += 1 }
    i==9 
    // for(i <- 0 until 9; j <- 0 until 9) if(contents(i)(j) == 0) return false
    // true
  }

  // Find a blank position; precondition: complete returns false
  def nextPos : (Int,Int) = {
    // for(i <- 0 until 9; j <- 0 until 9) if(contents(i)(j) == 0) return (i,j)
    // throw new RuntimeException("No blank position")
   // Much faster with a while loop
    var i = 0; var j = 0
    while(i < 9 && contents(i)(j) != 0){
      if(j < 8) j += 1 else{ j = 0; i += 1 }
    }
    assert(i < 9)
    (i, j)
  }

  // Can we play value d in position (i,j); precondition: (i,j) is blank
  def canPlay(i:Int, j:Int, d:Int) : Boolean = {
    // Check if d appears in row i
    var j1 = 0
    while(j1 < 9 && contents(i)(j1) != d) j1 += 1
    if(j1 < 9) false
    else{
      // check if d appears in column j
      var i1 = 0
      while(i1 < 9 && contents(i1)(j) != d)  i1 += 1 
      if(i1 < 9) false
      else{
	// Check if d appears in 3x3 block
	val basei = i/3*3; val basej = j/3*3
	i1 = basei; j1 = basej
	while(i1 < basei+3 && contents(i1)(j1) != d){
	  if(j1 < basej+2) j1 += 1 else { j1 = basej; i1 += 1 }
	}
	i1 == basei+3 // if got to basei+3 then d doesn't appear
      }
    }
  }

  // Create a new partial solution, extending this one by playing d in
  // position (i,j)
  def play(i:Int, j:Int, d:Int) : Partial = {
    val p = new SimplePartial
    for(i1 <- 0 until 9; j1 <- 0 until 9) 
      p.contents(i1)(j1) = contents(i1)(j1)
    p.contents(i)(j) = d
    p
  }
}
