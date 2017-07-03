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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class  ClassWordCount  extends Configured implements Tool{
	//	private static final String BASE_PATH = "hdfs://localhost:9000/user/hadoop/NaiveBayes/";
	private static final String INPUT_PATH = ClassifierMain.BASE_PATH + "WordNumInClass";
	private static final String OUTPUT_PATH = ClassifierMain.BASE_PATH+ "ClassWordCount";

	public static class ClassWordCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
		// 记录类名
		private Text classID = new Text();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
		}
		public void map(Text key,IntWritable value, Context context)
				throws IOException, InterruptedException {

			// 获得文件名和文件上级目录名，分别用作docID和classID
			String[] classAndFile = key.toString().split("@");
			String className = classAndFile[0];			

			this.classID.set(className );
	//		System.out.println(" className: "+className+"  ");
			context.write(this.classID, value);			
		}
	}
	public static class ClassWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable temp : value) {
				sum += temp.get();
			}
	//		System.out.println("sum : "+sum);
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

		Job job = new Job(conf, "ClassWordCount");

		job.setJarByClass(ClassWordCount.class);
		job.setMapperClass(ClassWordCountMapper.class);
		job.setCombinerClass(ClassWordCountReducer.class);
		job.setReducerClass(ClassWordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public void main(String[] args) throws Exception {
		System.out.println("ClassWordCount");
		 ToolRunner.run(new Configuration(),new ClassWordCount(), args);
	//	TestFileIsExist reader = new TestFileIsExist();
	//	reader.readFile(OUTPUT_PATH+"/part-r-00000");
	//	System.exit(res);

	}
}
