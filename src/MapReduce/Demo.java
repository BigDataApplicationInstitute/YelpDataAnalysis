package MapReduce;

import Parser.*;
import Property.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

 
public class Demo {
    
	public static class JsonMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, NullWritable value, Context context) throws IOException, InterruptedException {
			try {
	     
	            Gson gson = new Gson();
	            JsonReader reader = new JsonReader(new StringReader(value.toString()));
	            reader.setLenient(true);
	            
	            Type listType = new TypeToken<ArrayList<Business>>() {
                }.getType();
                List<Business> yourClassList = new Gson().fromJson(value.toString(), listType);
                for(Business list : yourClassList)
                {
                	context.write(new Text(list.stars), new Text(list.businessId));
                	
                }
	            Business property  = gson.fromJson(reader,Business.class); 
	            
	            if(property != null)
	            {
	            	//context.write(new Text(property.stars), new Text(property.stars));
	            }
	            
	           
	            
	            

			} catch (JSONException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();

        }
    }
}
	
    public static void main(String[] args) throws Exception {
    	 runJob(args[0], args[1]);
    }
 

    
    public static void runJob(String input, String output) throws Exception {
    	
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(Demo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(JsonMapper.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
    }
}

