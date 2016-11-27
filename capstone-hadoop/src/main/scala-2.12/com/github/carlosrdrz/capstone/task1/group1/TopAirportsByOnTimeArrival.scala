package com.github.carlosrdrz.capstone.task1.group1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object TopAirportsByOnTimeArrival {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf, "Top Airports By On Time Arrival")

    job.setMapperClass(classOf[AirlinesArrivalDelayMapper])
    job.setReducerClass(classOf[TopTenAvgReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setJarByClass(getClass)
    job.setNumReduceTasks(1)

    val exit = if (job.waitForCompletion(true)) 0 else 1
    System.exit(exit)
  }
}