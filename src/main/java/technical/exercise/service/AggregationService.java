package technical.exercise.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.*;
import static technical.exercise.constant.Constants.*;

public class AggregationService {

    public void rankedByOnlineAge(final Dataset<Row> ds) {
        Dataset<Row> countByStation = ds.groupBy(STATION).count()
                .sort(desc("count"))
                .withColumnRenamed("count", "NUMBER OF MONTH ONLINE");

        System.out.println("Rank by Station: descending order");
        countByStation.show(STATION_LIST.size());

        exportResult(countByStation, "rankOnlinePeriod");
    }

    public void rankByRainfall(final Dataset<Row> ds) {
        Dataset<Row> rankByRainfallByStation = ds.groupBy(STATION)
                .agg(sum(TOTAL_RAINFALL_MM).as(TOTAL_RAINFALL_MM))
                .sort(desc(TOTAL_RAINFALL_MM));

        System.out.println("Rank station by total rainfall mm: descending order");
        rankByRainfallByStation.show(STATION_LIST.size());

        exportResult(rankByRainfallByStation, "rankByRainfallByStation");
    }

    public void rankBySunshine(final Dataset<Row> ds) {
        Dataset<Row> rankBySunshineByStation = ds.groupBy(STATION)
                .agg(sum(TOTAL_SUNSHINE_HOURS).as(TOTAL_SUNSHINE_HOURS))
                .sort(desc(TOTAL_SUNSHINE_HOURS));

        System.out.println("Rank station by total sunshine hours: descending order");
        rankBySunshineByStation.show(STATION_LIST.size());

        exportResult(rankBySunshineByStation, "rankBySunshineByStation");
    }

    //TODO
    public void findPattern() {
    }

    public void worstRainfall(final Dataset<Row> ds) {
        Dataset<Row> maxRainfallByStation = ds.groupBy(STATION)
                .agg(max(col(TOTAL_RAINFALL_MM).cast("double")).as("MAX_RAINFALL_MM"));

        Dataset<Row> worstRainfallByStation = ds.alias("all").select(STATION, TOTAL_RAINFALL_MM, YEAR, MONTH)
                .join(maxRainfallByStation.alias("aggr"), col(TOTAL_RAINFALL_MM).equalTo(col("MAX_RAINFALL_MM"))
                        .and(col("all.STATION").equalTo(col("aggr.STATION"))))
                .select("all.STATION", YEAR, MONTH, "MAX_RAINFALL_MM");

        System.out.println("Worst rainfall for each station");
        worstRainfallByStation.show(STATION_LIST.size());

        exportResult(worstRainfallByStation, "worstRainfallByStation");
    }

    public void bestSunshine(final Dataset<Row> ds) {
        Dataset<Row> maxSunshineByStation = ds.groupBy(STATION)
                .agg(max(col(TOTAL_SUNSHINE_HOURS).cast("double")).as("MAX SUNSHINE HOUR"));

        Dataset<Row> bestSunshineByStation = ds.alias("all").select(STATION, TOTAL_SUNSHINE_HOURS, YEAR, MONTH)
                .join(maxSunshineByStation.alias("aggr"), col(TOTAL_SUNSHINE_HOURS).equalTo(col("MAX SUNSHINE HOUR"))
                        .and(col("all.STATION").equalTo(col("aggr.STATION"))))
                .select("all.STATION", YEAR, MONTH, "MAX SUNSHINE HOUR");

        System.out.println("Best sunshine for each station");
        bestSunshineByStation.show(STATION_LIST.size());

        exportResult(bestSunshineByStation, "bestSunshineByStation");
    }

    public void averageRainfallForMay(final Dataset<Row> ds) {
        Dataset<Row> averageRainfallForMay = ds.filter(col(MONTH).equalTo(lit("5")))
                .groupBy(YEAR).agg(avg(TOTAL_RAINFALL_MM).as("AVERAGE RAINFALL"));

        Dataset<Row> worstAverageRainfallYearForMay = averageRainfallForMay       // most rainfall
                .sort(desc("AVERAGE RAINFALL")).limit(1);

        Dataset<Row> bestAverageRainfallYearForMay = averageRainfallForMay       // least rainfall
                .sort("AVERAGE RAINFALL").limit(1);

        System.out.println("Average rainfall for May across all stations");
        averageRainfallForMay.show(200);
        exportResult(averageRainfallForMay, "averageRainfallForMay");

        System.out.println("Worst average rainfall year for May across all stations");
        worstAverageRainfallYearForMay.show();
        exportResult(worstAverageRainfallYearForMay, "worstAverageRainfallYearForMay");

        System.out.println("Best average rainfall year for May across all stations");
        bestAverageRainfallYearForMay.show();
        exportResult(bestAverageRainfallYearForMay, "bestAverageRainfallYearForMay");
    }

    private void exportResult(final Dataset<Row> ds, final String filePath) {
        ds.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv("aggregationResult/" + filePath);
    }

}
