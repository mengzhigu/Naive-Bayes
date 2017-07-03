package hadoop.NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountKind  extends Configured implements Tool{
//	private static final String BASE_PATH = "hdfs://localhost:9000/user/hadoop/NaiveBayes/";
	private static final String INPUT_PATH = ClassifierMain.BASE_PATH + "WordNumInClass";
	private static final String OUTPUT_PATH = ClassifierMain.BASE_PATH+ "TmpKindOfWord";

	public static class CountKindMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
		private Text Text = new Text();
		// 记录出现次数
	//	private IntWritable Num = new IntWritable(1);
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
		}
		public void map(Text key,IntWritable value, Context context)
				throws IOException, InterruptedException {
	//		System.out.println("value "+value);
			if(value.equals(new IntWritable(0))){
			//	System.out.println("value = 0");
				return;
			}
			else{
				String[] classAndAttribute = key.toString().split("@");
				String words = classAndAttribute[1];			
	//			System.out.println("words: "+words);

				this.Text.set(words);
				//			System.out.println(" className: "+className+"  ");
				context.write(this.Text, value);			
				//						System.out.println(word.toString());word
				//	}
			}
			
		}
	}
	public static class CountKindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			//Text name = new Text("[v]");
			int sum = 0;
			for (IntWritable temp : value) {
				sum += temp.get();
			}
			this.sum.set(sum);
			context.write(key, this.sum);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path outputPath = new Path(OUTPUT_PATH);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = new Job(conf, "CountKind");

		job.setJarByClass(CountKind.class);
		job.setMapperClass(CountKindMapper.class);
		job.setCombinerClass(CountKindReducer.class);
		job.setReducerClass(CountKindReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public void main(String[] args) throws Exception {
		System.out.println("CountKind");
		ToolRunner.run(new Configuration(),new CountKind(), args);
	//	TestFileIsExist reader = new TestFileIsExist();
	//	reader.readFile(OUTPUT_PATH+"/part-r-00000");
	//	System.exit(res);

	}
}
