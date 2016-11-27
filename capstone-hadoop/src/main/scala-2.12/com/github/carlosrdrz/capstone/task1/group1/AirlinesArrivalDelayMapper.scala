package com.github.carlosrdrz.capstone.task1.group1

import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class AirlinesArrivalDelayMapper extends Mapper[Object, Text, Text, DoubleWritable] {
  val SEPARATOR = ","

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, DoubleWritable]#Context): Unit = {
    // values(7) -> UniqueCarrier
    // values(6) -> ArrDelayMinutes
    val values = value.toString.split(SEPARATOR)
    val minutes = if (values(6).isEmpty) "0.00" else values(6)
    context.write(new Text(values(7)), new DoubleWritable(minutes.toDouble))
  }
}
