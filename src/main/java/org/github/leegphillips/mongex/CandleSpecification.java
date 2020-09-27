package org.github.leegphillips.mongex;

import java.time.LocalDateTime;

public interface CandleSpecification {
    CandleSize getTickSize();

    LocalDateTime getFloor(LocalDateTime time);

    LocalDateTime getCeiling(LocalDateTime floor);

    int getEventsPerDay();
}
