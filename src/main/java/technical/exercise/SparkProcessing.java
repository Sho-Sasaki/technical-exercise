package technical.exercise;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import technical.exercise.service.AggregationService;
import technical.exercise.service.Client;

import java.io.File;
import java.io.IOException;

import static com.google.common.io.Files.createTempDir;
import static technical.exercise.constant.Constants.COLUMN;
import static technical.exercise.constant.Constants.STATION_LIST;

public class SparkProcessing {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkProcessing.class);

    private static File file = file();
    private static Client client = new Client(file);

    private static File file() {
        final File file = createTempDir();
        file.deleteOnExit();
        return new File(file, "weatherData.csv");
    }

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().appName("Technical exercise").master("local[*]").getOrCreate();
        final Dataset<Row> ds = getWeatherDataSet(sparkSession);

        sparkSession.sparkContext().setLogLevel("ERROR");

        final AggregationService service = new AggregationService();

        service.rankedByOnlineAge(ds);
        service.rankByRainfall(ds);
        service.rankBySunshine(ds);

        //TODO
//        service.worstRainfall(ds);
//        service.bestSunshine(ds);
        service.averageRainfallForMay(ds);

        FileSystemUtils.deleteRecursively(file);
    }

    private static Dataset<Row> getWeatherDataSet(final SparkSession sparkSession) {
        try {
            loadHistoricalWeatherData();
        } catch (IOException e) {
            LOGGER.error("Error accessing csv file saved from met office.", e);
        }

        final Seq<String> seqColumn = JavaConverters.asScalaBuffer(COLUMN).toSeq();
        return sparkSession.read().csv(file.getPath()).toDF(seqColumn);
    }

    private static void loadHistoricalWeatherData() throws IOException {
        for (String station : STATION_LIST) {
            client.loadAllHistoricalWeatherData(station);
        }
    }
}
