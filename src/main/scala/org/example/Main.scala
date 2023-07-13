package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  def main(args:Array[String]):Unit = {
    implicit val spark = SparkSession.builder().appName("Probable Duplicates Grouper").master("local").getOrCreate()
    val userDetails = spark.read.option("header","true").csv("src/main/resources/input.csv").withColumn("id",monotonically_increasing_id())

    val inputColumns = userDetails.columns
    lazy val probableMatchQueryColumns = args(0).split(",")

    if(args.length > 0 && probableMatchQueryColumns.toSet.subsetOf(inputColumns.toSet)) {
      println("Will process")
    } else {
      throw new Exception("User query columns are inappropriate!")
    }

    import spark.implicits._

    val crossJoinedUserDetails = userDetails.as("left").crossJoin(userDetails.as("right"))
    crossJoinedUserDetails.cache()
    var updatedCrossJoinedUserDetails = crossJoinedUserDetails
    probableMatchQueryColumns.foreach(column=> {
      updatedCrossJoinedUserDetails = calculateLevenshtein(updatedCrossJoinedUserDetails,column)
    })
    updatedCrossJoinedUserDetails.show
    val probableMatchingRows = updatedCrossJoinedUserDetails.withColumn("avg_lev",expr("("+probableMatchQueryColumns.map(column=>"lev_"+column).reduce(_+"+"+_)+")"))
//      .filter("avg_lev <=2 AND "+probableMatchQueryColumns.map(column=>"lev_"+column+" <= 2").reduce(_+" AND "+_))
      .filter("avg_lev <= "+probableMatchQueryColumns.length*0.2)
      .selectExpr("left.id as leftId","right.id as rightId")
      .withColumn("set",array_sort(array("leftId","rightId")))

    val idSet = probableMatchingRows.select("leftId","set").union(probableMatchingRows.select("rightId","set"))
      .select($"leftId",explode($"set").as("set"))

    userDetails.join(defineGroupsForIds(idSet),Seq("id"),"inner").write.option("header","true").mode("overwrite").csv("src/main/resources/output/")
  }

  def calculateLevenshtein(df:DataFrame,column:String):DataFrame = {
    df.withColumn("lev_"+column,
      (levenshtein(coalesce(lower(col("left."+column)),lit("")),coalesce(lower(col("right."+column)),lit(""))))
        /
        ((length(coalesce(col("left."+column),lit("")))+length(coalesce(col("right."+column),lit(""))))/2))
  }

  def defineGroupsForIds(df:DataFrame)(implicit sparkSession: SparkSession):DataFrame = {
    import sparkSession.implicits._
    val aggSets = df.groupBy("leftId").agg(collect_set("set").as("set"))

    val ids = aggSets.agg(collect_list("set").as("sets")).collectAsList()

    val values = ids.get(0).getAs[Seq[Seq[Long]]](0)
    val idValues:Array[Array[Long]] = values.map(_.toArray).toArray

    val groups = idValues.foldLeft(Array(Array(-1L)))((a:Array[Array[Long]],b:Array[Long])=> {
      if(a.length==1) {
        a ++ Array(b)
      } else {
        val groupFound = a.filter(arrs=>arrs.intersect(b).length>0)
        if(groupFound.length > 0) {
          val matchingArray = a.filter(arrs=>arrs.intersect(b).length>0)(0)
          val index = a.indexOf(matchingArray)
          a(index)= matchingArray ++b
          a
        } else {
          a ++ Array(b)
        }
      }
    })

    val groupsDF = groups.map(a=>a.toSet).toSeq.toDF("groups")
      .select(explode($"groups").as("id"),array_join($"groups","").as("groupId"))

    groupsDF
  }
}
