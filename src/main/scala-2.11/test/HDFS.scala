package test


import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
  * @author wkd
  */
object HDFS {

  private val conf = new Configuration();

  conf.addResource(new Path("/home/hadoop/hd2.7/hadoop-2.7.1/etc/hadoop/core-site.xml"));
  conf.addResource(new Path("/home/hadoop/hd2.7/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));

  private val HDFS = FileSystem.get(conf);

  def removeFile(filename: String): Boolean = HDFS.delete(new Path(filename), true)

  def existFile(filename: String) = HDFS.exists(new Path(filename))

}