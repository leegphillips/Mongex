package org.github.leegphillips.mongex;

import org.bson.BsonArray;

import java.io.IOException;

public class RetailSalesexAutosMoM {
    public static void main(String[] args) throws IOException {
        String command = "curl \"https://calendar-api.fxstreet.com/en/api/v1/events/27f14eda-e042-4f9c-8e96-1a94022eba00/historical\" -H \"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0\" -H \"Accept: */*\" -H \"Accept-Language: en-GB,en;q=0.5\" -H \"Referer: https://www.fxstreet.com/economic-calendar/event/27f14eda-e042-4f9c-8e96-1a94022eba00?timezoneOffset=0\" -H \"Origin: https://www.fxstreet.com\" -H \"Connection: keep-alive\" -H \"Cache-Control: max-age=0\" -H \"TE: Trailers\"";
        BsonArray applications = new EventFetcher().fetch(command);
        System.out.println(applications.size());
    }
}
