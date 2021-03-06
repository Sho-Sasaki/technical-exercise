package technical.exercise.constant;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String URL = "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/";

    public static final List<String> STATION_LIST = Arrays.asList(
            "aberporth", "armagh", "ballypatrick", "bradford", "braemar", "camborne", "cambridge", "cardiff", "chivenor",
            "cwmystwyth", "dunstaffnage", "durham", "eastbourne", "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars",
            "lowestoft", "manston", "nairn", "newtonrigg", "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield",
            "southampton", "stornoway", "suttonbonington", "tiree", "valley", "waddington", "whitby", "wickairport", "yeovilton"
    );

    public static final String STATION = "STATION";
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    private static final String MEAN_MAX_TEMPERATURE = "MEAN_MAX_TEMPERATURE";
    private static final String MEAN_MIN_TEMPERATURE = "MEAN_MIN_TEMPERATURE";
    private static final String DAYS_OF_AIR_FROST = "DAYS_OF_AIR_FROST";
    public static final String TOTAL_RAINFALL_MM = "TOTAL_RAINFALL_MM";
    public static final String TOTAL_SUNSHINE_HOURS = "TOTAL_SUNSHINE_HOURS";

    public static final List<String> COLUMN = Arrays.asList(
            STATION,
            YEAR,
            MONTH,
            MEAN_MAX_TEMPERATURE,
            MEAN_MIN_TEMPERATURE,
            DAYS_OF_AIR_FROST,
            TOTAL_RAINFALL_MM,
            TOTAL_SUNSHINE_HOURS);
}
