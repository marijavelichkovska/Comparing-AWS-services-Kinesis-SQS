import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.Scanner;

import org.joda.time.DateTime;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.Axis;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;
 
 
public class LineChartSample extends Application {
 
    @Override public void start(Stage stage) throws FileNotFoundException {
        stage.setTitle("Line Chart Sample");
        //defining the axes
        final CategoryAxis xAxis = new CategoryAxis();
        final NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("Time");
        yAxis.setLabel("ms");
       
        //creating the chart
        final LineChart<String,Number> lineChart = 
                new LineChart<String,Number>(xAxis,yAxis);
                
        lineChart.setTitle("Latency of SQS");
        //defining a series
        XYChart.Series series = new XYChart.Series();
 
        File resultFile = new File("D:\\ResultsToGraph.txt");
	    Scanner reader = new Scanner(resultFile);

	      while (reader.hasNextLine()) {
	        String data = reader.nextLine();
	        String[] splitted = data.split(",");
	        series.getData().add(new XYChart.Data<String, Number>(splitted[0], Integer.parseInt(splitted[1])));
	      }
	    series.setName("Results");
	      
	    reader.close();
        Scene scene  = new Scene(lineChart,1000,800);
        lineChart.getData().add(series);
       
        stage.setScene(scene);
        stage.show();
    }
 
    public static void main(String[] args) {
        launch(args);
    }
}