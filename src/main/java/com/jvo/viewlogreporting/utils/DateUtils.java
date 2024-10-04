package com.jvo.viewlogreporting.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    private static DateTimeFormatter slashFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm");

    public static String asSlashedString(LocalDateTime localDateTime) {
        String formattedDate = slashFormat.format(localDateTime);
        return formattedDate;
    }
}
