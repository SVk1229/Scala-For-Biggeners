package ScalaBasics

object Vals_Variables_Types {

  /* Val: "val" in scala is equivalent to "final" keyword in java. Val is immutable
 *				if we assign a value to val then we can't reassign a value to val.
 */
  val intalue1: Int = 10
  // val -> val keyword, numericValue1 -> name, Int -> type, 10 -> value

  // Scala compiler can infer the type, it means even if we don't mention the type, it can infer.
  val shortValue2 = 4
  val longValue = 10202022202123L
  val floatValue = 10.22f
  val doubleValue = 302.22222123
  val booleanValue = false
  val stringValue = "stringValue"
  val charValues = "c"

  //Var is same like val, but vars are mutables, we can reassign the values to var.
  var a = "sai"
  a = "krishna"

}