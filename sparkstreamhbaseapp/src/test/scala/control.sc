object control {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  println("helo")                                 //> helo
  
  def main(args: Array[String]) {
        println( "Returned Value : " + addInt(5,7) );
   }                                              //> main: (args: Array[String])Unit
   
   def addInt( a:Int, b:Int ) : Int = {
      var sum:Int = 0
      sum = a + b

      return sum
   }                                              //> addInt: (a: Int, b: Int)Int
  
}