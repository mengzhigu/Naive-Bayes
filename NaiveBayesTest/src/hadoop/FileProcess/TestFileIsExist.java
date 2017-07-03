package hadoop.FileProcess;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;

public class TestFileIsExist {

	private static final String BASE_PATH = "hdfs://localhost:9000/user/hadoop/CFT/";
	private static final String RESULT_CLASSIFICATION = BASE_PATH+"WordNumInClass/part-r-00000";//"ResultOfClassification/part-r-00000"; //+"Dict/classDict.list"; 
	//"hdfs://localhost:9000/user/hadoop/Chinesefilestest/InputSequenceTestData/C000008/part-r-00000";

	@SuppressWarnings("deprecation")
	public void readFile(String rPath) throws IOException{
		Configuration conf = new Configuration();	    
		Path paths = new Path(rPath);
		FileSystem fs = paths.getFileSystem(conf);
		SequenceFile.Reader reader = null;
		System.out.println("Starting...");
		File targetDirFile = new File("/home/mjq/resultFile.txt");
		if(!targetDirFile.exists()){
			targetDirFile.createNewFile();
		}
		try {
			//FileSystem fs = FileSystem.get(conf);
			reader = new SequenceFile.Reader(fs, paths, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			//				BytesWritable value = (BytesWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			int count = 0;

			FileWriter tgWriter= new FileWriter("/home/mjq/resultFile.txt");

			while(reader.next(key, value)) {
				count ++;
				String syncSeen = reader.syncSeen() ? "*" : "";
				position = reader.getPosition(); //begingning of next record
				tgWriter.append(key+"   " +value+"\n");
			}
			tgWriter.flush();
			tgWriter.close();
			System.out.println(count + "...................................................." + count);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("End.......");
			IOUtils.closeStream(reader);
		}
	}
	public static void main(String[] args) throws IOException{
		TestFileIsExist reader = new TestFileIsExist();
		reader.readFile(RESULT_CLASSIFICATION);
	}
}
