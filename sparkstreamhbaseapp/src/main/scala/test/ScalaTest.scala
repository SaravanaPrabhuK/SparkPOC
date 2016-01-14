package test

object ScalaTest {
  
  def main(args: Array[String]){
    println(add(1,2,3,4,5));
  }
  
  def add(args : Int*) : Int = {
    
    var result:Int = 0
    
    for(arg <- args)
    {
      println(arg);
      result = result + arg
    }
      
    return result
      
  }
  
}