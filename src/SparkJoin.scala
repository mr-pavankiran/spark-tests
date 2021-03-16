
import org.apache.spark.sql.SparkSession;

object SparkJoin {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Join test").getOrCreate()
      
  val studentDataframe1=spark.read.option("delimiter"," ").option("header","true").csv("/user/pavan/student/student-20-records.csv")

//val studentDataframe1=spark.read.option("header","true").csv("/user/pavan/student/student-20-records.csv")

val studentDataframe2=spark.read.option("delimiter"," ").option("header","true").csv("/user/pavan/student/student-20-records.csv")

//val studentDataframe2=spark.read.option("header","true").csv("/user/pavan/student/student-20-records.csv")

val resultDataframe=studentDataframe1.join(studentDataframe2, studentDataframe1("sex") === studentDataframe2("sex"),"inner")

println("count from spark:"+resultDataframe.count())


  }
}
