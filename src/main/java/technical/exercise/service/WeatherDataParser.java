package technical.exercise.service;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static technical.exercise.constant.Constants.COLUMN;

class WeatherDataParser {

    WeatherDataParser() {
    }

    List<String> parseWeatherData(final BufferedReader br, final String station) {
        return br.lines()
                .filter(isMetrics())
                .map(String::trim)
                .map(l -> l.replaceAll("\\s+", ",")         // separate to create csv file
                        .replaceAll("[^(0-9.\\-,)]", ""))   // strip out special character
                .map(l -> {
                    final String lineWithStationColumn = station + "," + l;
                    if (lineWithStationColumn.split(",").length != COLUMN.size()) {
                        return lineWithStationColumn + ", ";
                    } else {
                        return lineWithStationColumn;
                    }
                })
                .collect(Collectors.toList());
    }

    private static Predicate<String> isMetrics() {
        return line -> Arrays.stream(line.split("\\s+"))
                .map(String::trim)
                .filter(StringUtils::isNotEmpty)
                .allMatch(l -> l.matches("-?\\d+(.\\d+[$*#]?)?|-+|Provisional"));
    }
}
