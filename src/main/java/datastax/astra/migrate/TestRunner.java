package datastax.astra.migrate;
//Java program to demonstrates ScheduleThreadPoolExecutor
//class
import java.util.*;
import java.util.concurrent.*;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;

class TestRunner {	
	public static void main(String[] args) 
	{
//		convert(Instant.class,"2019-12-13T00:00:00Z");
//		System.out.println(LocalDate.class.getSimpleName());
//		System.out.println(convert(LocalDate.class,"2019-12-13"));
//		Instant instant = Instant.parse("2021-05-06 00:00:00.000000+0000 ");
		String renameFile = "D:\\ScyllaDB\\Project\\Anant\\git-code\\test\\test.txt"+"_bkp";
        File file = new File("D:\\ScyllaDB\\Project\\Anant\\git-code\\test\\test.txt");
        File rename = new File(renameFile);
        if(rename.exists()) {
        	rename.delete();
        }
        boolean flag = file.renameTo(rename);
        if (flag == true) {
            System.out.println("File Successfully Renamed to : "+renameFile);
        }
        else {
            System.out.println("Operation Failed to rename file : ");
        }
//		PropertyEditor editor = PropertyEditorManager.findEditor(BigInteger.class);
//		System.out.println("editor "+editor);
//		
		
	}
	private static Object convert(Class<?> targetType, String text) {
		System.out.println("diksha text" + text);
		PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
		System.out.println("editor "+editor);
		editor.setAsText(text);
		Instant instant
        = Instant.parse("2022-03-24");
		return editor.getValue();
	}

//	public static void main(String[] args)
//	{
//		// Creating a ScheduledThreadPoolExecutor object
//		ScheduledThreadPoolExecutor threadPool
//			= new ScheduledThreadPoolExecutor(1);
//
//		// Creating two Runnable objects
//		Runnable task1 = new Command("task1");
//
//		// Printing the current time in seconds
//		System.out.println(
//			"Current time:"
//			+ Calendar.getInstance().get(Calendar.SECOND));
//
//		// Scheduling the first task which will execute
//		// after 2 seconds and then repeats periodically with
//		// a period of 8 seconds
//		threadPool.scheduleAtFixedRate(task1, 2, 8,
//									TimeUnit.SECONDS);
//
//		// Wait for 30 seconds
//		try {
//			Thread.sleep(30000);
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		// Remember to shut sown the Thread Pool
//		threadPool.shutdown();
//	}
}
//
////Class that implements Runnable interface
//class Command implements Runnable {
//	String taskName;
//	int index = 0;
//	public Command(String taskName)
//	{
//		this.taskName = taskName;
//	}
//	public void run()
//	{
//		try {
//			index++;
//			System.out.println("Task name : "
//							+ this.taskName + " " + index
//							+ " Current time : "
//							+ Calendar.getInstance().get(
//									Calendar.SECOND));
//			Thread.sleep(2000);
//			System.out.println("Executed : " + this.taskName
//							+ " Current time : "
//							+ Calendar.getInstance().get(
//									Calendar.SECOND));
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}

