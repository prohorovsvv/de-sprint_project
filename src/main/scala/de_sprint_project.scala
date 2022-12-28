import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object de_sprint_project extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("projekt_5")
    .getOrCreate()

  //Читаем файл локально
  val originalTaxiTable = spark.read
    .option("inferShema", "true")
    .option("header", true)
    .csv("src/main/yellow_tripdata_2020-01.csv")


  //Создаем вью с помощью метода spark.sql, отбрасывая выбросы данных и избыточную информацию
  originalTaxiTable.createOrReplaceTempView("taxiTrip")
  val taxi = spark.sql("select date(tpep_pickup_datetime) as start_date,\n" +
    "           date(tpep_dropoff_datetime) as end_date,\n" +
    "           passenger_count,\n " +
    "          total_amount\n" +
    "    from taxiTrip\n " +
    "   where date(tpep_pickup_datetime) >= '2020-01-01' and date(tpep_pickup_datetime) <= '2020-01-31'")
  //

  //  Группируем все поездки по датам
  val cntGroupByDay = taxi.groupBy("start_date")
    .count()
    .orderBy("start_date")

  //  Создаем DF с количеством поездок без пассажиров
  val noPassnСnt = taxi.filter(taxi("passenger_count") === 0)
    .orderBy("start_date")
    .groupBy("start_date")
    .agg(count("passenger_count").alias("no_passn_cnt"),
      min("total_amount").alias("min_amount_no_passn"),
      max("total_amount").alias("max_amount_no_passn"))

  //  Создаем DF с количеством поездок c одним пассажиром
  val onePassnCnt = taxi.filter(taxi("passenger_count") === 1)
    .orderBy("start_date")
    .groupBy("start_date")
    .agg(count("passenger_count").alias("one_passn_cnt"),
      min("total_amount").alias("min_amount_one_passn"),
      max("total_amount").alias("max_amount_one_passn"))

  //
  //  Создаем DF с количеством поездок c двумя пассажирами
  val twoPassnCnt = taxi.filter(taxi("passenger_count") === 2)
    .orderBy("start_date")
    .groupBy("start_date")
    .agg(count("passenger_count").alias("two_passn_cnt"),
      min("total_amount").alias("min_amount_two_passn"),
      max("total_amount").alias("max_amount_two_passn"))

  //  Создаем DF с количеством поездок c тремя пассажирами
  val threePassnCnt = taxi.filter(taxi("passenger_count") === 3)
    .orderBy("start_date")
    .groupBy("start_date")
    .agg(count("passenger_count").alias("three_passn_cnt"),
      min("total_amount").alias("min_amount_three_passn"),
      max("total_amount").alias("max_amount_three_passn"))

  //  Создаем DF с количеством поездок c 4+ пассажирами
  val fourPlusPassnCnt = taxi.filter(taxi("passenger_count") >= 4)
    .orderBy("start_date")
    .groupBy("start_date")
    .agg(count("passenger_count").alias("four_plus_passn_cnt"),
      min("total_amount").alias("min_amount_four_plus_passn"),
      max("total_amount").alias("max_amount_four_plus_passn"))

  //  Джоин всех таблиц по дате поездки
  val joinDF = cntGroupByDay.join(noPassnСnt, ("start_date"))
    .join(onePassnCnt, ("start_date"))
    .join(twoPassnCnt, ("start_date"))
    .join(threePassnCnt, ("start_date"))
    .join(fourPlusPassnCnt, ("start_date"))
    .orderBy("start_date")

  //  Считаем процент
  val precentage = joinDF.withColumn("percentage_zero", round(joinDF("no_passn_cnt") / joinDF("count") * 100))
    .withColumn("percentage_1p", round(joinDF("one_passn_cnt") / joinDF("count") * 100))
    .withColumn("percentage_2p", round(joinDF("two_passn_cnt") / joinDF("count") * 100))
    .withColumn("percentage_3p", round(joinDF("three_passn_cnt") / joinDF("count") * 100))
    .withColumn("percentage_4p_plus", round(joinDF("four_plus_passn_cnt") / joinDF("count") * 100))

  //  Строим витрину
  val df = precentage.select("start_date",
    "percentage_zero", "min_amount_no_passn", "max_amount_no_passn",
    "percentage_1p", "min_amount_one_passn", "max_amount_one_passn",
    "percentage_2p", "min_amount_two_passn", "max_amount_two_passn",
    "percentage_3p", "min_amount_three_passn", "max_amount_three_passn",
    "percentage_4p_plus", "min_amount_four_plus_passn", "max_amount_four_plus_passn")
    .withColumnRenamed("start_date", "date")
    .orderBy("start_date")
  df.show()

  //  Сохраняем результат в паркетник в папке с проектом
  df.write
    .mode("overwrite")
    .parquet("table.parquet")

}