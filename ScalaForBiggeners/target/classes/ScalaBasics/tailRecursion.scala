package ScalaBasics

object tailRecursion extends App {

  def Trecursion(x: Int, accumlator: BigInt): BigInt = {
    if (x <= 1) {
      accumlator
    } else {
      Trecursion(x - 1, x * accumlator)
    }
    
  }
println(Trecursion(9000,1))
}