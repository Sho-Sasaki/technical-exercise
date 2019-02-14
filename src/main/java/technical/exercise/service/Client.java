package technical.exercise.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Retryable;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import static technical.exercise.constant.Constants.URL;

public class Client {

    private final File file;

    private final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public Client(final File file) {
        this.file = file;
    }

    @Retryable
    public void loadAllHistoricalWeatherData(final String station) throws IOException {
        final java.net.URL url = new URL(URL + station + "data.txt");

        try (final InputStream inputStream = url.openStream();
             final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))) {
            final List<String> parsedData = new WeatherDataParser().parseWeatherData(br, station);
            br.close();

            final BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            writeToFile(parsedData, bw);
            bw.close();
            LOGGER.info("Finish parsing file for station={}", station);
        } catch (RuntimeException e) {
            LOGGER.error("Cannot fetch data source from {}", url, e);
        }
    }

    private void writeToFile(final List<String> data, final BufferedWriter writer) throws IOException {
        for (String line : data) {
            writer.append(line);
            writer.newLine();
        }

    }
}
