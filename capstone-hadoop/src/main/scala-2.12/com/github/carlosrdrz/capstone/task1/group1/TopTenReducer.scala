package com.github.carlosrdrz.capstone.task1.group1

import java.lang.Iterable

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._
import scala.collection.mutable

class TopTenReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val results = new mutable.TreeMap[String, Int]

  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    results.put(key.toString, values.asScala.map(i => i.get()).sum)
  }

  override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    results
      .toList
      .sortWith((left, right) => left._2 > right._2)
      .take(10)
      .foreach((result) => context.write(new Text(result._1), new IntWritable(result._2)))
  }
}
