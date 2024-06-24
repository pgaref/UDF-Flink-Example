package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DurationToMinutesFunction extends ScalarFunction {

    public static final String NAME = "DURATION_TO_MINUTES";

    // Define the regex pattern for ISO 8601 duration
    private static final Pattern DURATION_PATTERN = Pattern.compile(
            "^P(?:([0-9]+)D)?(?:T(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?)?$"
    );

    public Integer eval(String duration) {
        if (duration == null || duration.isEmpty()) {
            return null;
        }

        Matcher matcher = DURATION_PATTERN.matcher(duration);
        if (!matcher.matches()) {
            return null;
        }

        int days = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 0;
        int hours = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
        int minutes = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
        int seconds = matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : 0;

        // Calculate total minutes
        int totalMinutes = days * 24 * 60 + hours * 60 + minutes + seconds / 60;

        return totalMinutes;
    }

    public static void main(String[] args) {
        DurationToMinutesFunction durationToMinutesFunction = new DurationToMinutesFunction();
        System.out.println(durationToMinutesFunction.eval("PT1H30M"));
        System.out.println(durationToMinutesFunction.eval("P2DT3H4M5S"));
        System.out.println(durationToMinutesFunction.eval("P1D"));
        System.out.println(durationToMinutesFunction.eval("P5D"));
    }
}