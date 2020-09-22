package org.github.leegphillips.mongex;

import org.bson.BsonArray;

import java.io.IOException;

public class MBAMortgageApplications {
    public static void main(String[] args) throws IOException {
        String command = "curl \"https://calendar-api.fxstreet.com/en/api/v1/events/0ee35d01-eb52-4133-8acc-53ea6361930b/historical\" -H \"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0\" -H \"Accept: */*\" -H \"Accept-Language: en-GB,en;q=0.5\" -H \"Referer: https://www.fxstreet.com/economic-calendar/event/0ee35d01-eb52-4133-8acc-53ea6361930b?timezoneOffset=0\" -H \"Origin: https://www.fxstreet.com\" -H \"Connection: keep-alive\" -H \"Cache-Control: max-age=0\" -H \"TE: Trailers\"";
        BsonArray applications = new EventFetcher().fetch(command);
        System.out.println(applications.size());
    }
}
