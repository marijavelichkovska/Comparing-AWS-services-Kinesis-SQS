import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.Scanner;
import org.joda.time.DateTime;

public class ParseResultSQSFile {

	public static void main(String[] args) throws IOException {
		try {
		    FileWriter fileWriter;
		    BufferedWriter bufferedWriter;
		    File resultsFile = new File("D:\\ResultsSQS.txt");
		    Scanner reader = new Scanner(resultsFile);
		      
		      fileWriter = new FileWriter("D:\\ResultsToGraph.txt", true);
	          bufferedWriter = new BufferedWriter(fileWriter);
	            
		      while (reader.hasNextLine()) {
		        String data = reader.nextLine();
		        String[] splitted = data.split(",");
		        
		        DateTime send = new DateTime (splitted[1]);
		        DateTime received = new DateTime(splitted[2]);
		        
		        long difference = received.minus(send.getMillis()).getMillis();
		        System.out.println(); 
		        received.toLocalTime();
		        String result = received.toLocalTime().toString() + "," + difference;
		        bufferedWriter.write(result);
		        bufferedWriter.newLine();
		      }
		      
		      reader.close();
		      bufferedWriter.flush();
		      bufferedWriter.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
	}
}
