package main

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.time.format.DateTimeFormatter
import java.time.LocalDate

object ReducerJoin {
	def main(args: Array[String]) {

		val userFile = args(0)
		val friendsFile = args(1)
		val outputFile = args(2)
		
		val conf = new SparkConf().setAppName("maxage")
		val sc = new SparkContext(conf)
		val input =  sc.textFile(friendsFile)

		val userinput = sc.textFile(userFile)

		val userdetail = userinput.map(line => (line.split(",")(0), line))
		
		val user = input.map(line => (line.split("\t")(0), 
							if (line.split("\t").length > 1 )  
								line.split("\t")(1).split(",") 
							else 
								Array[String]() )).filter(x => x._2.length > 0)
		
		val reversefriend = user.flatMap(x=> (x._2.map(y=> (y,x._1))))
		
		val joinrdd = userdetail.join(reversefriend)
			
		val userage = joinrdd.map(x => 
									(x._2._2, (x._2._1.split(",")(3), 2016 - x._2._1.split(",")(9).split("/")(2).toInt) ) )
		
		val descmaxage = userage.reduceByKey((x,y)=> if (x._2 > y._2) (x._1,x._2) else (y._1,y._2))
								.sortBy(_._2._2, false).map(x=> x._1+"\t"+x._2._1+", "+x._2._2)
		descmaxage.saveAsTextFile(outputFile)		
		descmaxage.take(10).foreach(println)
	}
}