package com.github.carlosrdrz.capstone.task1

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.mapreduce.Mapper

class SplitterMapper extends Mapper[Object, Text, Text, IntWritable] {
  val SEPARATOR = ","

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    // line(3) -> Origin
    // line(4) -> Dest
    val values = value.toString.split(SEPARATOR)

    Array(values(3), values(4))
      .foreach((airport) => context.write(new Text(airport), new IntWritable(1)))
  }
}
