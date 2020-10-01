package com.github.leegphillips.mongex;

import java.time.LocalDateTime;

public interface CandleSpecification {
    TimeFrame getTickSize();

    LocalDateTime getFloor(LocalDateTime time);

    LocalDateTime getCeiling(LocalDateTime floor);

    int getEventsPerDay();
}
