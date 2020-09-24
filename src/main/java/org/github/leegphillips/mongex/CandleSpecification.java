package org.github.leegphillips.mongex;

import java.time.LocalDateTime;

public interface CandleSpecification {
    String getTickSize();

    LocalDateTime getFloor(LocalDateTime time);

    LocalDateTime getCeiling(LocalDateTime floor);

    int getEventsPerDay();
}
