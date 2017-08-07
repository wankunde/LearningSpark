/**
  * Created by jointwisdom on 2017/8/4 004.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DataType

/**
  * Spark SQL UDAS：user defined aggregation function
  * UDF: 函数的输入是一条具体的数据记录，实现上讲就是普通的scala函数-只不过需要注册
  * UDAF：用户自定义的聚合函数，函数本身作用于数据集合，能够在具体操作的基础上进行自定义操作
  */
object SparkSQLUDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLWindowFunctionOps")
    val sc = new SparkContext(conf)

    val hiveContext = new SQLContext(sc)

    val bigData = Array("Spark","Hadoop","Flink","Spark","Hadoop","Flink","Sp1sdfasdf231231ark","Hadoop","Flink","Spark","Hadoop","Flink")
    val bigDataRDD = sc.parallelize(bigData)

    val bigDataRowRDD = bigDataRDD.map(line => Row(line))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val bigDataDF = hiveContext.createDataFrame(bigDataRowRDD, structType)

    bigDataDF.registerTempTable("bigDataTable")

    /*
     * 通过HiveContext注册UDF，在scala2.10.x版本UDF函数最多可以接受22个输入参数
     */
//    hiveContext.udf.register("computeLength",(input:String) => input.length)
//    hiveContext.sql("select name,computeLength(name)  as length from bigDataTable group by name").show

    hiveContext.udf.register("wordCount",new MyUDAF)
    hiveContext.sql("select wordCount(name) as maxLength from bigDataTable").show
  }
}

/**
  * 用户自定义函数
  */
class MyUDAF extends UserDefinedAggregateFunction
{
  /**
    * 指定具体的输入数据的类型
    * 自段名称随意：Users can choose names to identify the input arguments - 这里可以是“name”，或者其他任意串
    */
  override def inputSchema:StructType = StructType(Array(StructField("name",StringType,true)))

  /**
    * 在进行聚合操作的时候所要处理的数据的中间结果类型
    */
  override def bufferSchema:StructType = StructType(Array(StructField("maxLength",IntegerType,true)))

  /**
    * 返回类型
    */
  override def dataType:DataType = IntegerType

  /**
    * whether given the same input,
    * always return the same output
    * true: yes
    */
  override def deterministic:Boolean = true

  /**
    * Initializes the given aggregation buffer
    */
  override def initialize(buffer:MutableAggregationBuffer):Unit = {
    buffer(0)=0
  }

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    */
  override def update(buffer:MutableAggregationBuffer,input:Row):Unit={
    val bufLength = buffer.getInt(0);
    val inputLength = input.getString(0).length()
    if(bufLength < inputLength){
      buffer(0) = inputLength
    }
  }

  /**
    * 最后在分布式节点进行local reduce完成后需要进行全局级别的merge操作
    */
  override def merge(buffer1:MutableAggregationBuffer,buffer2:Row):Unit={
    val a = buffer1.getInt(0)
    val b = buffer2.getInt(0)
    if(a < b){
      buffer1.update(0,b)
    }
  }

  /**
    * 返回UDAF最后的计算结果
    */
  override def evaluate(buffer:Row):Any = buffer.getInt(0)
}
