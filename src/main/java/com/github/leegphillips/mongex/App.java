package com.github.leegphillips.mongex;

import com.github.leegphillips.mongex.ma.MALoader;

public class App {
    public static void main(String[] args) {
        CandleLoader.main(args);
        FullCandleSeriesChecker.main(args);
        MALoader.main(args);
    }
}
