package solutions.assignment2;

// Azizul Hakim Shakil <azizul_hakim.shakil@mailbox.tu-dresden.de>

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution2
{
	
    /* your code goes in here*/
	
	//Mapper
	public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
	{
        private final static IntWritable one = new IntWritable(1);

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	try {
                String valueString = value.toString();
                String[] singleTaxiData = valueString.split(",");
                String data = singleTaxiData[1];
    			int time_hour  = Integer.parseInt(data.substring(11,13));
    			int time_min = Integer.parseInt(data.substring(14,16));

    			if ( (0<=time_hour && time_hour<1) && (0<=time_min && time_min<=59) ){
					context.write(new Text("12am"), one);
				}
    			
				else if ( (1<=time_hour && time_hour<2) && (0<=time_min && time_min <=59) ){
					context.write(new Text("1am"), one);		
				}
    			
				else if ( (2<=time_hour && time_hour<3) && (0<=time_min && time_min <=59) ){
					context.write(new Text("2am"), one);		
				}

				else if ( (3<=time_hour && time_hour<4) && (0<=time_min && time_min <=59) ){
					context.write(new Text("3am"), one);		
				}

				else if ( (4<=time_hour && time_hour<5) && (0<=time_min && time_min <=59) ){
					context.write(new Text("4am"), one);		
				}
				
				else if ( (5<=time_hour && time_hour<6) && (0<=time_min && time_min <=59) ){
					context.write(new Text("5am"), one);		
				}
				
				else if ( (6<=time_hour && time_hour<7) && (0<=time_min && time_min <=59) ){
					context.write(new Text("6am"), one);		
				}
				
				else if ( (7<=time_hour && time_hour<8) && (0<=time_min && time_min <=59) ){
					context.write(new Text("7am"), one);		
				}
				
				else if ( (8<=time_hour && time_hour<9) && (0<=time_min && time_min <=59) ){
					context.write(new Text("8am"), one);		
				}
				
				else if ( (9<=time_hour && time_hour<10) && (0<=time_min && time_min <=59) ){
					context.write(new Text("9am"), one);		
				}
				
				else if ( (10<=time_hour && time_hour<11) && (0<=time_min && time_min <=59) ){
					context.write(new Text("10am"), one);		
				}
				
				else if ( (11<=time_hour && time_hour<12) && (0<=time_min && time_min <=59) ){
					context.write(new Text("11am"), one);		
				}
				
				else if ( (12<=time_hour && time_hour<13) && (0<=time_min && time_min <=59) ){
					context.write(new Text("12pm"), one);		
				}
				
				else if ( (13<=time_hour && time_hour<14) && (0<=time_min && time_min <=59) ){
					context.write(new Text("1pm"), one);		
				}
				
				else if ( (14<=time_hour && time_hour<15) && (0<=time_min && time_min <=59) ){
					context.write(new Text("2pm"), one);		
				}
				
				else if ( (15<=time_hour && time_hour<16) && (0<=time_min && time_min <=59) ){
					context.write(new Text("3pm"), one);		
				}
				
				else if ( (16<=time_hour && time_hour<17) && (0<=time_min && time_min <=59) ){
					context.write(new Text("4pm"), one);		
				}
				
				else if ( (17<=time_hour && time_hour<18) && (0<=time_min && time_min <=59) ){
					context.write(new Text("5pm"), one);		
				}
				
				else if ( (18<=time_hour && time_hour<19) && (0<=time_min && time_min <=59) ){
					context.write(new Text("6pm"), one);		
				}
				
				else if ( (19<=time_hour && time_hour<20) && (0<=time_min && time_min <=59) ){
					context.write(new Text("7pm"), one);		
				}
				
				else if ( (20<=time_hour && time_hour<21) && (0<=time_min && time_min <=59) ){
					context.write(new Text("8pm"), one);		
				}
				
				else if ( (21<=time_hour && time_hour<22) && (0<=time_min && time_min <=59) ){
					context.write(new Text("9pm"), one);		
				}
				
				else if ( (22<=time_hour && time_hour<23) && (0<=time_min && time_min <=59) ){
					context.write(new Text("10pm"), one);		
				}
				
				else if ( (23<=time_hour && time_hour<24) && (0<=time_min && time_min <=59) ){
					context.write(new Text("11pm"), one);		
				}
				
				else {
					context.write(new Text("Missing some values"), one);		
				}
        		
        	}catch (Exception e){
        		
        	}
            
        }

	 }
	
    //Reducer
    public static class ReduceRecords extends Reducer<Text, IntWritable, Text, IntWritable>
        {

            protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException
            {
                int sum = 0;
            
                for (IntWritable val : values)
                sum += val.get();
                
                context.write(key, new IntWritable(sum));
            }
        }
    
    /* */

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        /* your code goes in here */
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(MapRecords.class);
        job.setReducerClass(ReduceRecords.class);

         /* */
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};
        
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}

