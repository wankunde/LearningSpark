
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
	* Created by wang on 2017/9/7.
	*/
object MakeHBDSFeatureLabels {
	private def make_labels_schema(htlCds: Seq[Int]): StructType = {
		StructType(
			StructField("live_dt", StringType, nullable = false) +: htlCds.flatMap { hcd =>
				Seq(StructField(s"${hcd}_rns", FloatType, nullable = false),
					StructField(s"${hcd}_rev", FloatType, nullable = false))
			}
		)
	}

	def etl(sc: SparkContext, srcDir: String, dstDir: String, dt: String,
					htlCds: Seq[Int] = Seq[Int](), save: Boolean = true): Unit = {

		val ssc = new SQLContext(sc)

		import ssc.implicits._

		val nintyDaysBefore = TimeUtils.someday(-90, TimeUtils.dtToDate(dt), "yyyy-MM-dd")

		println("90 days before--->" + nintyDaysBefore)

		val htlCdStr = htlCds.mkString(",")

		val bkDailySumDF = sc.textFile(srcDir)
														.map(_.split("#"))
															.map(cols => HtlBkDailySum(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8).toFloat, cols(9).toFloat, cols(10).toInt))
																  .toDF()

		bkDailySumDF.registerTempTable("table_htlbkdailysum")

		val sumRnsTable = bkDailySumDF.select($"htl_cd", $"seg_cd", $"chn_cd", $"rm_typ", $"rt_cd", $"is_member", $"order_dt", $"live_dt", $"rns", $"rev", $"stat_hour")
		                                 .where(s"htl_cd in ($htlCdStr) and live_dt>='$nintyDaysBefore'")

		sumRnsTable.show()

		val sumRnsRevTable = sumRnsTable.groupBy($"live_dt", $"htl_cd").agg(sum($"rns") as "rns", sum($"rev") as "rev")
		sumRnsRevTable.show()

		//生成90天的date表

		val test = sumRnsRevTable.select($"htl_cd").map(row => row.getAs[String]("htl_cd")).distinct().collect().toList

		val test2 = sumRnsRevTable.groupBy($"live_dt").pivot("htl_cd", test).agg(sum("rns"), sum("rev")).na.fill(0).orderBy("live_dt")

		test2.show()
	}

	def main(args: Array[String]): Unit = {

			val sfg = new SparkConf()
					.setAppName("MakeHtlBkDailySumLabelsEtl")
					.setMaster("local[2]")
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			val srcDir = "./src/main/resources/data/htl_bk_daily_sum/dt=20170903/*/*" //cfg.getString("app.src.folder.htl_bk_daily_sum") + s"/dt=$dt/*/*"
			val dstDir = "./src/main/resources/data/htl_bk_daily_sum/output"

			val sc = new SparkContext(sfg)
			MakeHBDSFeatureLabels.etl(sc, srcDir, dstDir, "20170903", htlCds = Seq(101537, 221065))
	}


}
