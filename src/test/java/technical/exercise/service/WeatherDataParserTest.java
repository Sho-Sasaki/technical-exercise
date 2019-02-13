package technical.exercise.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.BufferedReader;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

class WeatherDataParserTest {

    @Mock
    private BufferedReader br;

    private WeatherDataParser parser;

    @BeforeEach
    void setUp() {
        initMocks(this);
        parser = new WeatherDataParser();
    }

    //TODO
    @Test
    void parseWeatherData() {
        final List<String> result = parser.parseWeatherData(br, "station");
    }
}