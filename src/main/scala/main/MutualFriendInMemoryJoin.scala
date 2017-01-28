package main

import org.apache.spark._
import org.apache.spark.SparkContext._

object MutualFriendInMemoryJoin {
    def main(args: Array[String]) {
      val userFile = args(0)
      val friendsFile = args(1)
      val outputFile = args(2)
      val conf = new SparkConf().setAppName("mutualfriend")
      val sc = new SparkContext(conf)
      val input =  sc.textFile(friendsFile)
      
      val userbroadcastFile = sc.broadcast(userFile)
      val userinput = sc.textFile(userbroadcastFile.value)

      var userA = -1
      var userB = -1
      if(args.length > 3) {
    	userA = args(3).toInt;
    	userB = args(4).toInt;
      }
      
      val _user = input.map(line => (line.split("\t")(0), 
    		  if (line.split("\t").length > 1 )  
    		 	  line.split("\t")(1).split(",") 
    		  else 
    		 	  Array[String]() ))
      
      val user = _user.filter(x => if(userA != -1) (x._1.toInt == userA || x._1.toInt == userB) else true)
    		 	  
      val usertuple = user
      				.flatMap(userfriend => userfriend._2
      						.map(friend => 
      							if (friend < userfriend._1 ) 
      								((friend, userfriend._1), userfriend._2)  
      							else 
      								((userfriend._1, friend), userfriend._2)))
      							
      
      val mutualfriend = usertuple
      						.filter(
      							x => if(userA != -1){
      									if (userA < userB)(x._1._1.toInt == userA && x._1._2.toInt == userB) 
      									else (x._1._1.toInt == userB && x._1._2.toInt == userA)
      									}
      								else true
      							)
      						.reduceByKey((x, y) => (x.intersect(y)))
      						
      val mapmutualfriend = mutualfriend.flatMap(x => (x._2.map(y => (y,x._1))))
      
      val userdetail = userinput.map(line => (line.split(",")(0), line))
      
      val result = userdetail.join(mapmutualfriend)
      val res = result.map(line => (line._2._2, line._2._1.split(",")(1)+":"+line._2._1.split(",")(9)))
      							.reduceByKey((x,y) => (x+", "+y)).map(x=> x._1._1+"\t"+x._1._2+"\t["+x._2+"]")
      
      res.saveAsTextFile(outputFile)
    }
}