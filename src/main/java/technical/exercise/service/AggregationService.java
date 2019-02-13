package technical.exercise.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static technical.exercise.constant.Constants.*;

public class AggregationService {

    private static Logger LOGGER = LoggerFactory.getLogger(AggregationService.class);

    public void rankedByOnlineAge(final Dataset<Row> ds) {
        Dataset<Row> countByStation = ds.groupBy(STATION).count()
                .sort(desc("count"))
                .withColumnRenamed("count", "NUMBER OF MONTH ONLINE");

        LOGGER.info("Rank by Station: descending order");
        countByStation.show(STATION_LIST.size());

        exportResult(countByStation, "countByStation");
    }

    public void rankByRainfall(final Dataset<Row> ds) {
        Dataset<Row> rankByRainfall = ds.groupBy(STATION)
                .agg(sum(TOTAL_RAINFALL_MM).as(TOTAL_RAINFALL_MM))
                .sort(desc(TOTAL_RAINFALL_MM));

        LOGGER.info("Rank by rainfall: descending order");
        rankByRainfall.show(STATION_LIST.size());

        exportResult(rankByRainfall, "rankByRainfall");
    }

    public void rankBySunshine(final Dataset<Row> ds) {
        Dataset<Row> rankBySunshine = ds.groupBy(STATION)
                .agg(sum(TOTAL_SUNSHINE_HOURS).as(TOTAL_SUNSHINE_HOURS))
                .sort(desc(TOTAL_SUNSHINE_HOURS));

        LOGGER.info("Rank by rainfall: descending order");
        rankBySunshine.show(STATION_LIST.size());

        exportResult(rankBySunshine, "rankBySunshine");
    }

    //TODO
    public void findPattern() {
    }

    public void worstRainfall(final Dataset<Row> ds) {
        final Seq<String> joinCols = JavaConverters.asScalaBuffer(Arrays.asList(STATION, TOTAL_RAINFALL_MM)).toSeq();

        Dataset<Row> maxRainfallByStation = ds.groupBy(STATION)
                .agg(max(TOTAL_RAINFALL_MM).as(TOTAL_RAINFALL_MM));
        ds.select(STATION, YEAR, MONTH).join(maxRainfallByStation, joinCols);

        LOGGER.info("Worst rainfall");
        maxRainfallByStation.show(STATION_LIST.size());

        exportResult(maxRainfallByStation, "maxRainfallByStation");
    }

    public void bestSunshine(final Dataset<Row> ds) {
        final Seq<String> joinCols = JavaConverters.asScalaBuffer(Arrays.asList(STATION, TOTAL_SUNSHINE_HOURS)).toSeq();

        Dataset<Row> maxSunshineByStation = ds.groupBy(STATION).agg(max(TOTAL_SUNSHINE_HOURS).as(TOTAL_SUNSHINE_HOURS));
        ds.select(STATION, YEAR, MONTH).join(maxSunshineByStation, joinCols);

        LOGGER.info("Best rainfall");
        maxSunshineByStation.show(STATION_LIST.size());

        exportResult(maxSunshineByStation, "maxSunshineByStation");
    }

    public void averageRainfallForMay(final Dataset<Row> ds) {
        Dataset<Row> averageRainfallForMay = ds.filter(col(MONTH).equalTo(lit("5")))
                .groupBy(YEAR, MONTH).agg(avg(TOTAL_RAINFALL_MM).as("AVERAGE RAINFALL"));

        Dataset<Row> worstAverageRainfallYearForMay = averageRainfallForMay       // most rainfall
                .sort(desc("AVERAGE RAINFALL")).limit(1);

        Dataset<Row> bestAverageRainfallYearForMay = averageRainfallForMay       // least rainfall
                .sort("AVERAGE RAINFALL").limit(1);

        LOGGER.info("Average rainfall for May");
        averageRainfallForMay.show(200);
        exportResult(averageRainfallForMay, "averageRainfallForMay");

        LOGGER.info("Worst average rainfall year for May");
        worstAverageRainfallYearForMay.show();
        exportResult(worstAverageRainfallYearForMay, "worstAverageRainfallYearForMay");

        LOGGER.info("Best average rainfall year for May");
        bestAverageRainfallYearForMay.show();
        exportResult(bestAverageRainfallYearForMay, "bestAverageRainfallYearForMay");
    }

    private void exportResult(final Dataset<Row> ds, final String filePath) {
        ds.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv("aggregationResult/" + filePath);
    }

}
