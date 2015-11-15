/** The Partial trait, corresponding to partial solutions of a Sudoku puzzle
 */

trait Partial {
  /** Initialise from a file */
  def init(fname: String)

  /** Print partial solution */
  def printPartial

  /** Is the partial solution complete? */
  def complete : Boolean

  /** Find a blank position */
  def nextPos : (Int,Int)

  /** Can we play value d in position (i,j)? */
  def canPlay(i: Int, j: Int, d: Int) : Boolean

  /** Create a new partial solution, extending this one by playing d in
    * position (i,j) */
  def play(i: Int, j: Int, d: Int) : Partial
}
