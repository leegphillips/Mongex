package org.github.leegphillips.mongex;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class CandleDefinitions {
    final static CandleSpecification ONE_M = new CandleSpecification() {
        @Override
        public String getTickSize() {
            return "1M";
        }

        @Override
        public LocalDateTime getFloor(LocalDateTime time) {
            return time.truncatedTo(ChronoUnit.MINUTES);
        }

        @Override
        public LocalDateTime getCeiling(LocalDateTime floor) {
            return floor.plus(Duration.ofMinutes(1));
        }
    };

    final static CandleSpecification FIVE_M = new CandleSpecification() {
        @Override
        public String getTickSize() {
            return "5M";
        }

        @Override
        public LocalDateTime getFloor(LocalDateTime time) {
            LocalDateTime current = time.truncatedTo(ChronoUnit.HOURS);
            LocalDateTime next = getCeiling(current);
            while (next.isBefore(time)) {
                current = next;
                next = getCeiling(current);
            }
            return current;
        }

        @Override
        public LocalDateTime getCeiling(LocalDateTime floor) {
            return floor.plus(Duration.ofMinutes(5));
        }
    };
}
