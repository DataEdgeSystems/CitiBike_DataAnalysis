package genderAnalytic;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.util.StringTokenizer;

public class genderAnalyticMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String one = "1";
	private Text word = new Text();

	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//System.out.println("LINE:\n"+line);
		String cvsSplitBy = ",";
		
		// Start of Code to Find the number of Male,Female and Unknown users
		String[] tokenized_data = line.split(cvsSplitBy);
		String current_Customer_Gender = tokenized_data[14]; //gender is in Index location 14
		if(current_Customer_Gender.matches("1")){
			//System.out.println("male block");
			context.write(new Text("male"),new Text(one));
		}
		else if(current_Customer_Gender.matches("2")){
			context.write(new Text("female"),new Text(one));
		}
		else if(current_Customer_Gender.matches("0")){
			context.write(new Text("unknown_gender"),new Text(one));
		}	
		// End of Code to Find the number of Male,Female and Unknown users
		
		
		// Start of Code to Find the number of Male,Female for each age ( also based on UserType)
		String current_birth_year = tokenized_data[13]; // Birth Year is in Index location 13
		String currentCustomerType = tokenized_data[12]; // UserType is in Index location 12
		if(current_birth_year.length()==4){
			String mapperOutputKey = new String();
			int currentAge = 2015 - Integer.parseInt(current_birth_year);
			if(currentCustomerType.equals("Customer") && current_Customer_Gender.equals("1")){
				//System.out.println("Male Customer emitted");
				mapperOutputKey = Integer.toString(currentAge)+":CM";
				
			}else if (currentCustomerType.equals("Customer") && current_Customer_Gender.equals("2")){
				//System.out.println("Female Customer emitted");
				mapperOutputKey = Integer.toString(currentAge)+":CF";
				
			}else if(currentCustomerType.equals("Subscriber") && current_Customer_Gender.equals("2")){
				//System.out.println("Female Subscriber emitted");
				mapperOutputKey = Integer.toString(currentAge)+":SF";
				
			}else if (currentCustomerType.equals("Subscriber") && current_Customer_Gender.equals("1")){
				//System.out.println("Male Subscriber emitted");
				mapperOutputKey = Integer.toString(currentAge)+":SM";
				
			}
			
			context.write(new Text(mapperOutputKey),new Text(one));
		}
		else{
			context.write(new Text("unknown_age"), new Text(one));
		
		}
		// End of Code to Find the number of Male,Female for each age ( also based on UserType)
		
		//Start of Code to find the total number of Subscribers and Customers
		String userType = tokenized_data[12].toString();
		if(userType.equals("Customer")){
			//System.out.println("Customer count");
			context.write(new Text("CustomerType"),new Text(one));
		}
		else if(userType.equals("Subscriber")){
			//System.out.println("Subscriber count");
			context.write(new Text("SubscriberType"),new Text(one));
		}
		
		//End of Code to find the total number of Subscribers and Customers
		//System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "M") + " Miles\n");
		//System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "K") + " Kilometers\n");
		//System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "N") + " Nautical Miles\n");		
		
		//Start of Code to find the average distance by Citi-Bike Users
		
		double startLatitude = Double.parseDouble(tokenized_data[5]);
		double startLongitude = Double.parseDouble(tokenized_data[6]);
		double endLatitude = Double.parseDouble(tokenized_data[9]);
		double endLongitude = Double.parseDouble(tokenized_data[10]);
		double totalDistance = distance(startLatitude,startLongitude,endLatitude,endLongitude,"M");
		//System.out.println(totalDistance);
		String currentUserTripDistance = Double.toString(totalDistance);
		context.write(new Text("*"),new Text(currentUserTripDistance));
		//End of Code to find the average distance by Citi-Bike Users
				
		
		//Start of Code to find the most popular stations
		int startStationID = Integer.parseInt(tokenized_data[3]);
		int endStationID = Integer.parseInt(tokenized_data[7]);
		context.write(new Text("#"+startStationID),new Text("S"));
		context.write(new Text("#"+endStationID),new Text("E"));
		
		
		//End of Code to find the most popular stations
	}
	/*::	This function computes the distance between two points using latitudes and longitudes						 :*/
	private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == "K") {
			dist = dist * 1.609344;
		} else if (unit == "N") {
			dist = dist * 0.8684;
		}

		return (dist);
	}

	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/*::	This function converts radians to decimal degrees						 :*/
	private static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}
	
	
	
	
}