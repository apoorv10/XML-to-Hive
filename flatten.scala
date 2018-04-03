/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package xml
 
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive._
import java.util.Properties
import java.io.FileInputStream
import java.io._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType,DataType,StructField, StringType, DoubleType,ArrayType,IntegerType, DateType, TimestampType};
import org.apache.spark.sql.functions.{array, col, explode, lit, coalesce}

 
object flatten {
 
  def main(args: Array[String]) {
 
    if (args.length < 1) {
      System.err.println("Usage: XMLParser.jar <config.properties>")
      println("Please provide the Configuration File for the XML Parser Job")
      System.exit(1)
    }
 
    val sc = new SparkContext(new SparkConf().setAppName("Spark XML Process"))
    val sqlContext = new HiveContext(sc)    
    val dfSchema = sqlContext.read.format("com.databricks.spark.xml").option("rowTag","AttributionPopulationGroup").load(args(0))
    
    val flattened_DataFrame=flattenDf(dfSchema)
  
  }
  
    
   
  def flattenDf(df: DataFrame): DataFrame = {
  var end = false 
  var i = 0
  val fields = df.schema.fields  
  val fieldNames = fields.map(f => f.name) 
  val fieldsNumber = fields.length 
 
  while (!end) {
    val field = fields(i) 
    val fieldName = field.name 
   
 
    field.dataType match {  
      case st: StructType =>
         val childFieldNames = st.fieldNames.map(n => fieldName + "." + n +" as "+ fieldName+"_"+n)
         val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
         newFieldNames.foreach(println)
         val newDf = df.selectExpr(newFieldNames: _*)
         return flattenDf(newDf)
       
      case at: ArrayType =>
        val fieldExplode = df.select(explode(col(s"$fieldName")).alias(s"$fieldName"))
        val fieldExplode1 = fieldExplode.selectExpr(s"$fieldName.*")
        val structFields = fieldExplode1.schema.simpleString
        val explodedDf = df.selectExpr((fieldNames: _*)).withColumn(s"$fieldName", explode(coalesce(col(s"$fieldName"),array(lit(null).cast(structFields)))))
        return flattenDf(explodedDf)
      case _ => Unit
    }
 
    i += 1
    end = i >= fieldsNumber
  }
  df
}
 
} 
