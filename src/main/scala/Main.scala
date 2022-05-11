import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.Tool

/**
 * mapper
 */
class Map1 extends Mapper[LongWritable, Text, Text, PhoneBean] {
  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable, Text, Text, PhoneBean]#Context): Unit = {
    //----------------------
    val phoneBean = new PhoneBean()
    val line = value.toString
    val infos = line.split("\\s+", 0)
    val infosReverse = infos.reverse

    phoneBean.setUpFlow(infosReverse(2).toInt)
    phoneBean.setDownFlow(infosReverse(1).toInt)
    phoneBean.setSumFlow(infosReverse(1).toInt + infosReverse(2).toInt)

    context.write(new Text(infos(1)), phoneBean)
  }
}
/**
 * reducer
 */
class Red1 extends Reducer[Text, PhoneBean, Text, PhoneBean] {
  def reduce(key: Text,
             values: Iterator[PhoneBean],
             context: Reducer[Text, PhoneBean, Text, PhoneBean]#Context): Unit = {

    //汇总个数
    var totalup: Long = 0
    var totaldown: Long = 0
    val newPhoneBean = new PhoneBean()

    for (elem <- values) {
      totalup += elem.getUpFlow()
      totaldown += elem.getDownFlow()
    }

    newPhoneBean.setUpFlow(totalup)
    newPhoneBean.setDownFlow(totaldown)
    newPhoneBean.setSumFlow()

    context.write(key, newPhoneBean)

  }
}
/**
 * 启动类
 */
object MrTest extends Configured with Tool{
  override def run(args: Array[String]): Int = {
    val conf = new Configuration()
    conf.set("fs.defaultFS","hdfs://emr-header-1.cluster-285604:9000")
    val job = Job.getInstance(conf)
    job.setJarByClass(this.getClass)
    job.setJobName("phoneCount")

    //map,reduce,combiner
    job.setMapperClass(classOf[Map1])
    job.setCombinerClass(classOf[Red1])
    job.setReducerClass(classOf[Red1])
    job.setNumReduceTasks(1)

    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[PhoneBean])
    //输入输出
    FileInputFormat.addInputPath(job,new Path("/zhushengping/phone.txt"))
    FileOutputFormat.setOutputPath(job,new Path("/zhushengping/result"))
    //等待作业完成
    val status:Boolean= job.waitForCompletion(true)
    //返回值： int 【0完成, 1 失败】
    if (status) 0 else 1
  }
  def main(args: Array[String]): Unit = {
    run(Array())
  }
}
