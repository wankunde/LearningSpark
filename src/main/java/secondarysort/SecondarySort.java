package secondarysort;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *  按公司分组，求出人流量前3的商品，输出格式为 公司ID, 商品ID , 人流量
 */
public class SecondarySort {
    private static SparkSession spark;

    static {
        spark = SparkSession
                .builder()
                .appName("Dataset-Java-Basic")
                .master("local[4]")
                .getOrCreate();
    }

    public static void achieveByDataSet(){

        JavaRDD<String> lines = spark.read().textFile("src/main/resources/data/shopPassager.txt").toJavaRDD();//文件存储格式 公司ID, 商铺ID, 人流量

        JavaPairRDD<String, String> pairs = lines.mapToPair(line ->{
            String [] fields = line.split(",");
            return new Tuple2<>(fields[0] + "-" + fields[1], fields[2]);
        });
//        JavaPairRDD rePartitionRdd = pairs.partitionBy(new HashPartitioner(4));

        JavaPairRDD rePartitionRdd =  pairs.groupByKey();
        System.out.println(rePartitionRdd.getClass().getName());

        rePartitionRdd.foreach(a ->{
            Tuple2 pair = (Tuple2) a;
            System.out.println(pair._1 + "，" + pair._2);
        });

    }

    public static void main(String[] args) {
        achieveByDataSet();
    }
}

class CompanyPartitioner extends Partitioner {
    public CompanyPartitioner() {
        super();
    }

    @Override
    public int numPartitions() {
        return 0;
    }

    @Override
    public int getPartition(Object key) {
        Tuple2<String, String> tuple =  (Tuple2<String, String>)key;
        System.out.println("tuple = [" + tuple._1 + "-" + tuple._2 + "]");
        return Integer.parseInt(tuple._1) / 10;
    }
}

