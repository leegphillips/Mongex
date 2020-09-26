package org.github.leegphillips.mongex;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@SuppressWarnings("unused")
public class CandleDefinitions {
    final static CandleSpecification ONE_MINUTE = new CandleSpecification() {
        @Override
        public CandleSize getTickSize() {
            return CandleSize.ONE_MINUTE;
        }

        @Override
        public LocalDateTime getFloor(LocalDateTime time) {
            return time.truncatedTo(ChronoUnit.MINUTES);
        }

        @Override
        public LocalDateTime getCeiling(LocalDateTime floor) {
            return floor.plus(Duration.ofMinutes(1));
        }

        @Override
        public int getEventsPerDay() {
            // not implemented
            return 0;
        }
    };

    final static CandleSpecification FIVE_MINUTES = new CandleSpecification() {
        @Override
        public CandleSize getTickSize() {
            return CandleSize.FIVE_MINUTES;
        }

        @Override
        public LocalDateTime getFloor(LocalDateTime time) {
            LocalDateTime current = time.truncatedTo(ChronoUnit.HOURS);
            LocalDateTime next = getCeiling(current);
            while (next.isBefore(time) || next.isEqual(time)) {
                current = next;
                next = getCeiling(current);
            }
            return current;
        }

        @Override
        public LocalDateTime getCeiling(LocalDateTime floor) {
            return floor.plus(Duration.ofMinutes(5));
        }

        @Override
        public int getEventsPerDay() {
            return 24 * 60 / 5;
        }
    };
}
