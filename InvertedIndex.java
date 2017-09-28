import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

		private Text docid = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			docid.set(itr.nextToken());
			while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, docid);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
						   
			HashMap<String, Integer> m=new HashMap<String, Integer>();
			int count=0;	
			for (Text value : values) {
				String str = value.toString();

				// System.out.println("LOG-ENTRY-IDENTIFIER: Reducer - " + key + "  " + new_str);
				
				// String[] key_vals = new_str.split(":");
				String docids = str+":";
				//System.out.println("LOG-ENTRY-IDENTIFIER: Reducer - " + key + "  " + docids);
				if(m!=null && m.get(docids)!=null){
				 
				count = (int)m.get(docids);

				m.put(docids, ++count);
				 
				}else{
				m.put(docids, 1);
				 
				}
				
			}
			String formatted_values = m.toString().replace("{", "").replace("}", "").replace("=", "").replace(", ","\t");
			
			context.write(key, new Text(formatted_values));
		}
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
	conf.set("mapreduce.textoutputformat.separator", "\t");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}