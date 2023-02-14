package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Date;
import java.util.NoSuchElementException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class Util {

    public static String getSparkProp(SparkConf sc, String prop) {
        try {
            return sc.get(prop);
        } catch (NoSuchElementException nse) {
            String newProp = prop.replace("origin", "source").replace("target", "destination");
            return sc.get(newProp);
        }
    }

    public static String getSparkPropOr(SparkConf sc, String prop, String defaultVal) {
        try {
            return sc.get(prop);
        } catch (NoSuchElementException nse) {
            String newProp = prop.replace("origin", "source").replace("target", "destination");
            return sc.get(newProp, defaultVal);
        }
    }

    public static String getSparkPropOrEmpty(SparkConf sc, String prop) {
        return getSparkPropOr(sc, prop, "");
    }

    public static BufferedReader getfileReader(String fileName) {
        try {
            return new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException("No '" + fileName + "' file found!! Add this file in the current folder & rerun!");
        }
    }
    private static void appendToFile(Path path, String content)
				throws IOException {
        // if file not exists, create and write to it
				// otherwise append to the end of the file
        Files.write(path, content.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
	}
    
    private static void writeToFile(Path path, String content)
			throws IOException {
    // if file not exists, create and write to it
			// otherwise override existing file
    Files.write(path, content.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);
    }
	
	public static ConsistencyLevel mapToConsistencyLevel(String level) {
        ConsistencyLevel retVal = ConsistencyLevel.LOCAL_QUORUM;
        if (StringUtils.isNotEmpty(level)) {
            switch (level.toUpperCase()) {
                case "ANY":
                    retVal = ConsistencyLevel.ANY;
                    break;
                case "ONE":
                    retVal = ConsistencyLevel.ONE;
                    break;
                case "TWO":
                    retVal = ConsistencyLevel.TWO;
                    break;
                case "THREE":
                    retVal = ConsistencyLevel.THREE;
                    break;
                case "QUORUM":
                    retVal = ConsistencyLevel.QUORUM;
                    break;
                case "LOCAL_ONE":
                    retVal = ConsistencyLevel.LOCAL_ONE;
                    break;
                case "EACH_QUORUM":
                    retVal = ConsistencyLevel.EACH_QUORUM;
                    break;
                case "SERIAL":
                    retVal = ConsistencyLevel.SERIAL;
                    break;
                case "LOCAL_SERIAL":
                    retVal = ConsistencyLevel.LOCAL_SERIAL;
                    break;
                case "ALL":
                    retVal = ConsistencyLevel.ALL;
                    break;
            }
        }

        return retVal;
    }
    
	private static final String NEW_LINE = System.lineSeparator();

    public static void FileAppend(String dir, String fileName, String content) throws IOException {
    	
    	//create directory if not already existing
    	Files.createDirectories(Paths.get(dir));
        Path path = Paths.get(dir + "/" + fileName);
        appendToFile(path, content + NEW_LINE);

    }
    
    public final static String getDateTime()
    {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_hh_mm_ss");
        return df.format(new Date());
    }
}
