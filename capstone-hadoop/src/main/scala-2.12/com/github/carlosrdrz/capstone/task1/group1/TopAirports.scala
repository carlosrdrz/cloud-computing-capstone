package com.github.carlosrdrz.capstone.task1.group1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object TopAirports {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf, "Top Airports")

    job.setMapperClass(classOf[AirportsMapper])
    job.setReducerClass(classOf[TopTenReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    job.setJarByClass(getClass)
    job.setNumReduceTasks(1)

    val exit = if (job.waitForCompletion(true)) 0 else 1
    System.exit(exit)
  }
}