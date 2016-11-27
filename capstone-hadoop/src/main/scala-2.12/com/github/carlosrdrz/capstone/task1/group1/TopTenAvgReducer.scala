package com.github.carlosrdrz.capstone.task1.group1

import java.lang.Iterable

import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._
import scala.collection.mutable

class TopTenAvgReducer extends Reducer[Text, DoubleWritable, Text, DoubleWritable] {
  val results = new mutable.TreeMap[String, Double]

  override def reduce(key: Text, values: Iterable[DoubleWritable], context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
    val vals = values.asScala.map(i => i.get())
    results.put(key.toString, vals.sum / vals.size)
  }

  override def cleanup(context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
    results
      .toList
      .sortWith((left, right) => left._2 < right._2)
      .take(10)
      .foreach((result) => context.write(new Text(result._1), new DoubleWritable(result._2)))
  }
}
