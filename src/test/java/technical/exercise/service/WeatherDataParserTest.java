package technical.exercise.service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.BufferedReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class WeatherDataParserTest {

    @Mock
    private BufferedReader bufferedReader;

    private WeatherDataParser parser;

    @Before
    public void setUp() {
        initMocks(this);
        bufferedReader = mock(BufferedReader.class);
        parser = new WeatherDataParser();
    }

    @Test
    public void parseWeatherData_shouldGenerateCommaSeparatedString() {
        final Stream<String> stringStream = Stream.of("   2018   6   19.1    12.4       0    10.2   270.2#  Provisional\n");
        when(bufferedReader.lines()).thenReturn(stringStream);

        final List<String> result = parser.parseWeatherData(bufferedReader, "station");
        assertThat(result.size()).isEqualTo(1);
        final List<String> expectedResult = Arrays.asList("station,2018,6,19.1,12.4,0,10.2,270.2");
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void givenSunHoursColumnIsMissingFromDataSource_parseWeatherData_shouldPopulateEmptyValueForSunHoursColumn() {
        final Stream<String> stringStream = Stream.of("   1961   9   18.6    10.0       0    26.9\n");
        when(bufferedReader.lines()).thenReturn(stringStream);

        final List<String> result = parser.parseWeatherData(bufferedReader, "station");
        assertThat(result.size()).isEqualTo(1);
        final List<String> expectedResult = Arrays.asList("station,1961,9,18.6,10.0,0,26.9, ");
        assertThat(result).isEqualTo(expectedResult);
    }

}