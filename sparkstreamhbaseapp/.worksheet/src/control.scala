object control {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(60); 
  println("Welcome to the Scala worksheet");$skip(18); 
  println("helo");$skip(97); 
  
  def main(args: Array[String]) {
        println( "Returned Value : " + addInt(5,7) );
   };System.out.println("""main: (args: Array[String])Unit""");$skip(108); 
   
   def addInt( a:Int, b:Int ) : Int = {
      var sum:Int = 0
      sum = a + b

      return sum
   };System.out.println("""addInt: (a: Int, b: Int)Int""")}
  
}
