# Databricks notebook source

# loaded cities files and created a temp table in _source database
spark.read.option("header", "true").csv("/mnt/inputs/de_intern_assignment_cities.csv").write.mode("overWrite").format("delta").saveAsTable("_source.cities")

#loaded rides file and created a temp table in _source database
spark.read.option("header", "true").csv("/mnt/inputs/de_intern_assignment_rides.csv").write.mode("overWrite").format("delta").saveAsTable("_source.rides")



spark.sql("select distinct ride_id, rider_id, vehicle_id, city_id, time_ride_start_local, time_ride_end_local, date(time_ride_start_local) day from _source.rides").write.mode("overWrite").format("delta").saveAsTable("_source.cleaned_rides")



spark.sql("select count(ride_id) as no_of_rides, count(rider_id) as no_of_riders, city_id, day from _source.cleaned_rides group by city_id,day order by day, city_id").write.mode("overWrite").format("delta").saveAsTable("_results.no_of_rides")


spark.sql("select city_id, round(count(ride_id)/7) as avg_rides, day date from _source.cleaned_rides group by city_id,day having cast(day as date) > day - interval 7 day order by day, city_id").write.mode("overWrite").format("delta").saveAsTable("_results.avg_ride")


spark.sql("select city_name, country_name from _source.cities").write.mode("overWrite").format("delta").saveAsTable("_results.city_details")

spark.table("_source.cleaned_rides").write.mode("overWrite").format("parquet").option("overWriteSchema", True).save("/mnt/outputs/rides")

spark.table("_source.cities").write.mode("overWrite").format("parquet").option("overWriteSchema", True).save("/mnt/outputs/cities")

spark.table("_results.no_of_rides").write.mode("overWrite").format("parquet").option("overWriteSchema", True).save("/mnt/outputs/no_of_rides")

spark.table("_results.avg_ride").write.mode("overWrite").format("parquet").option("overWriteSchema", True).save("/mnt/outputs/avg_rides")

spark.table("_results.city_details").write.mode("overWrite").format("parquet").option("overWriteSchema", True).save("/mnt/outputs/city_details")


