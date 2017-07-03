package hadoop.FileProcess;


import hadoop.DataProcess.ChineseTokenizer;
import hadoop.DataProcess.DefaultStopWordsHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DataFileInputFormat extends
		FileInputFormat<NullWritable, String> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, String> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		DataFileRecordReader reader = new DataFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}

}

class DataFileRecordReader extends RecordReader<NullWritable, String> {

	private FileSplit fileSplit;
	private Configuration conf;
	//private BytesWritable value = new BytesWritable();
	private String value = new String();
	private boolean processed = false;

	@Override
	public void close() throws IOException {
		// do nothing
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public String getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	// process表示记录是否已经被处理过
	@SuppressWarnings("deprecation")
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			Path file = fileSplit.getPath();
			String contents = new String();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
		//	System.out.println("begin to input files");
			try {
			in = fs.open(file);
			String con = in.readLine();
				while (con != null) {  
					String data = new String(con.getBytes("ISO-8859-1"),"GBK"); 
					contents= contents+data+"\r\n";  
					con=  in .readLine();  
				}  
			//	String contents = new String(value.getBytes("ISO-8859-1"),"GBK"); 
				List<String> wordsList =ChineseTokenizer.segStr(contents);//.keySet();
				List<String> set = DefaultStopWordsHandler.dropStopWords(wordsList);
				Iterator<String> iterator = set.iterator();
				String resString = new String();
				while(iterator.hasNext()){
					resString+=" "+iterator.next();
				}
				value = resString;
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				in.close();
			}
			processed = true;
			return true;
		}
		return false;
	}
}