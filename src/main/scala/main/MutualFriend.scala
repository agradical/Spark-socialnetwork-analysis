package main

import org.apache.spark._
import org.apache.spark.SparkContext._

object MutualFriend {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("mutualfriend")
      val sc = new SparkContext(conf)
      val input =  sc.textFile(inputFile)
      
      var userA = -1
      var userB = -1
      if(args.length > 2) {
    	userA = args(2).toInt;
    	userB = args(3).toInt;
      }
    	  
      val _user = input.map(line => (line.split("\t")(0), 
    		  if (line.split("\t").length > 1 )  
    		 	  line.split("\t")(1).split(",") 
    		  else 
    		 	  Array[String]() ))
    		 	  
   	  val user = _user.filter(x => if(userA != -1) (x._1.toInt == userA || x._1.toInt == userB) else true)
    		 	  
      val usertuple = user.filter(x => x._2.length > 0)
      				  .flatMap(userfriend => userfriend._2
      						.map(friend => 
      							if (friend < userfriend._1 ) 
      								((friend, userfriend._1), userfriend._2)  
      							else 
      								((userfriend._1, friend), userfriend._2)))
      							
      val mutualfriend = usertuple
      					.filter(x => if(userA != -1)
      									{	if (userA < userB)(x._1._1.toInt == userA && x._1._2.toInt == userB) 
      										else (x._1._1.toInt == userB && x._1._2.toInt == userA)
      									}
      								else true)
      					.reduceByKey((x, y) => (x.intersect(y)))
      val result = mutualfriend.map(x => x._1._1 +", "+ x._1._2 +"\t"+ x._2.mkString(", "))
      
      result.saveAsTextFile(outputFile)
      
    }
}