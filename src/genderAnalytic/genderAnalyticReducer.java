package genderAnalytic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class genderAnalyticReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String currentKey = key.toString();
		System.out.println("Current Key"+currentKey);
		int incrementCounter = 1;
		if(currentKey.contains(":")) // If : is present in the key, it means its the age which is the key 
									//and the value is the corresponding gender
		{
			/*
			int maleCount=0;
			int femaleCount=0;
			int unknownCount=0;
			// Start of Code to Find the number of Male,Female for each age
			String newKey = currentKey.replace(":", "");
			for (Text value : values) {
				String str = value.toString();
				if(str.contains("1")){
					System.out.println("***Male*****\n");
				maleCount += incrementCounter;
				}
				else if((str.contains("2"))){
					System.out.println("***Female*****\n");
					femaleCount += incrementCounter;
				}
				else if ((str.contains("0"))){
					System.out.println("***Unknown*****\n");
					unknownCount += incrementCounter;
				}
			}
			String finalMaleCount = Integer.toString(maleCount);
			String finalFemaleCount = Integer.toString(femaleCount);
			String finalUnknownCount = Integer.toString(unknownCount);
			String finalAgeOutput = "Male:"+ finalMaleCount + " \t Female:"+finalFemaleCount+" \tUnknown:"+finalUnknownCount;
			
			context.write(new Text(newKey), new Text(finalAgeOutput));
			// End of Code to Find the number of Male,Female for each age
			*/
						
			int maleCustomerCount=0;
			int femaleCustomerCount=0;
			int femaleSubscriberCount=0;
			int maleSubscriberCount=0;
			int unknownCount=0;
			// Start of Code to Find the number of Male,Female for each age
			String newKey = "Age:"+currentKey.substring(0,2);
			for (Text value : values) {				
				String str = key.toString();
				//System.out.println("Current Value"+str);
				if(str.contains(":CM")){
					//System.out.println("***Male Customer*****\n");
				maleCustomerCount += incrementCounter;
				}
				else if((str.contains(":SM"))){
					//System.out.println("***Male Subscriber*****\n");
					maleSubscriberCount += incrementCounter;
				}
				else if ((str.contains(":CF"))){
					//System.out.println("***Female Customer*****\n");
					femaleCustomerCount += incrementCounter;
				}
				else if ((str.contains(":SF"))){
					//System.out.println("***Female Subscriber*****\n");
					femaleSubscriberCount += incrementCounter;
				}
				else if((str.contains("unknown_age"))){
					//System.out.println("***Unknown Age*****\n");
					unknownCount += incrementCounter;
				}
			}
			int currentAgeTotal = maleSubscriberCount+maleCustomerCount+femaleCustomerCount+femaleSubscriberCount;
			String finalCurrentAgeTotal=Integer.toString(currentAgeTotal);
			String finalMaleSubscriberCount = Integer.toString(maleSubscriberCount);
			String finalMaleCustomerCount = Integer.toString(maleCustomerCount);
			String finalFemaleSubscriberCount = Integer.toString(femaleSubscriberCount);
			String finalFemaleCustomerCount = Integer.toString(femaleCustomerCount);
			String finalUnknownCount = Integer.toString(unknownCount);
			String finalAgeOutput = "****\tMale Subscriber:"+ finalMaleSubscriberCount +"\tMale Customer:"+ finalMaleCustomerCount+ " \t Female Customer:"+finalFemaleCustomerCount+" \t Female Subscriber:"+finalFemaleSubscriberCount+"\tTotal:"+finalCurrentAgeTotal+" \tUnknown:"+finalUnknownCount;
			
			context.write(new Text(newKey), new Text(finalAgeOutput));
					
			
		}
		else if(currentKey.contains("*")){
			Double totalTripDistance =0.00;
			int noOfTrips =0;
			Double averageTripDistance=0.00;
			for (Text value : values) {
				String str = value.toString();
				Double currentDistance = Double.parseDouble(str);
				//System.out.println("CurrentDistance"+currentDistance);
				totalTripDistance += currentDistance;
				noOfTrips += incrementCounter;
				//System.out.println("Trip #:"+noOfTrips);		
			}
			//System.out.println("TotalDistance"+totalTripDistance);
			//System.out.println("TripCount"+noOfTrips);
			averageTripDistance = totalTripDistance/noOfTrips;
			String avgDistance = Double.toString(averageTripDistance);
			context.write(new Text("Average Trip Distance"), new Text(avgDistance));
			
		}else if(currentKey.contains("$")){
			// Start of Code to find the the DAY of the week with most trips
			int mondayCount=0;
			int tuesdayCount=0;
			int wednesdayCount=0;
			int thursdayCount=0;
			int fridayCount=0;
			int saturdayCount=0;
			int sundayCount=0;
			String currentDay;
			for (Text value : values) {
				currentDay = value.toString();
				switch(currentDay){
				case "Monday":{
					mondayCount+=1;break;
				}
				case "Tuesday":{
					tuesdayCount+=1;break;
				}
				case "Wednesday":{
					wednesdayCount+=1;break;
				}
				case "Thursday":{
					thursdayCount+=1;break;
				}
				case "Friday":{
					fridayCount+=1;break;
				}
				case "Saturday":{
					saturdayCount+=1;break;
				}
				case "Sunday":{
					sundayCount+=1;break;
				}
				}
			}
			String outputKey="Week Stats::";
			String outputValue="Monday:"+mondayCount+"\tTuesday:"+tuesdayCount+"\tWednesday:"+wednesdayCount+"\tThursday"+thursdayCount+"\tFriday:"+fridayCount+"\tSaturday:"+saturdayCount+"\tSunday:"+sundayCount;
			context.write(new Text(outputKey),new Text(outputValue));
			// End of Code to find the the DAY of the week with most trips
		}
		else if(currentKey.contains("#")){
			System.out.println("Hash found");
			int startCounter=0;
			int endCounter=0;
			for (Text value : values) {
				String str = value.toString();
				//System.out.println("STR: "+str);
				if(str.equals("E")){
				endCounter += incrementCounter;}
				else if(str.equals("S")){
					startCounter+=incrementCounter;
				}
			}
			String outputKey = "Station ID:"+currentKey.replaceAll("#","");
			String outputValue = "Trips from:"+Integer.toString(startCounter)+"\t Trips to:"+Integer.toString(endCounter);
			context.write(new Text(outputKey), new Text(outputValue));
		}
		
		else{
			// Start of Code to Find the number of Male,Female and Unknown users
			
			int total = 0;
			
			for (Text value : values) {
				String str = value.toString();
				//System.out.println("STR: "+str);
				if(str.contains("1")){
				total += incrementCounter;}
			}
			String finalTotal = Integer.toString(total);
			context.write(key, new Text(finalTotal));
			// End of Code to Find the number of Male,Female and Unknown users
			
		}
		
		
		}
}