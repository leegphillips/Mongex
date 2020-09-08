package org.github.leegphillips.mongex.objectid;

import org.bson.types.ObjectId;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class MongoIdFactory {
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final String BASE_DATE = "2000-01-01";

    private final long baseTimeMillis;

    public MongoIdFactory() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(SIMPLE_DATE_FORMAT.parse(BASE_DATE));
        baseTimeMillis = calendar.getTimeInMillis();
    }

    public ObjectId getObjectId(String namespace, String pair) {
        // this will have to be recalc if any of this changes
        long aLong = new BigInteger(pair.getBytes()).intValue();
        return getObjectId(namespace, aLong + baseTimeMillis);
    }

    public ObjectId getObjectId(String namespace, long timestamp) {
        if (namespace.length() != 2) {
            throw new NumberFormatException("Namespace must be two chars: " + namespace);
        }

        long rebasedTimestamp = timestamp - baseTimeMillis;
        String rebasedHex = Long.toHexString(rebasedTimestamp);
        if (rebasedHex.length() > 10) {
            throw new NumberFormatException("Rebased hex timestamp must be < = 10 chars: " + rebasedHex + " " + namespace + " " + timestamp);
        }

        while (rebasedHex.length() < 10) {
            rebasedHex = "0" + rebasedHex;
        }

        byte[] bytes = (namespace + rebasedHex).getBytes();
        return new ObjectId(bytes);
    }

//    public static void main(String[] args) throws ParseException {
//        long now = System.currentTimeMillis();
//
//        long difference = now - bas;
//        System.out.println(difference);
//
//        String hexDiff = Long.toHexString(difference);
//        System.out.println(hexDiff);
//        System.out.println(hexDiff.length());
//
//        File[] files = new File(PropertiesSingleton.getInstance().getProperty(SOURCE_DIR)).listFiles();
//        System.out.println(files.length);
//        Stream<String> distinct = Stream.of(files).map(file -> file.getName().substring(19, 25)).distinct();
//        distinct.forEach(name -> System.out.println(name));
//
//        System.out.println(Stream.of(files).map(file -> file.getName().substring(19, 25)).distinct().count());
//    }
}
