package nta.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.Message;

public class FileUtil {
	public static void writeProto(File file, Message proto) throws IOException {
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(proto.toByteArray());
		stream.close();		
	}
	public static void writeProto(OutputStream out, Message proto) throws IOException {
		out.write(proto.toByteArray());
	}
	
	public static void writeProto(Configuration conf, Path path, Message proto) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream stream = fs.create(path);
		stream.write(proto.toByteArray());
		stream.close();
	}
	
	public static Message loadProto(File file, Message proto) throws IOException {
		FileInputStream in = new FileInputStream(file);
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);		
		return builder.build();
	}
	
	public static Message loadProto(InputStream in, Message proto) throws IOException {
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static Message loadProto(Configuration conf, Path path, Message proto) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream in = new FSDataInputStream(fs.open(path));
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static File getFile(String path) {
		return new File(path);
	}
	
	public final static String readTextFile(String filePath) throws IOException {
    StringBuffer fileData = new StringBuffer(1000);
    BufferedReader reader = new BufferedReader(
            new FileReader(filePath));
    char[] buf = new char[1024];
    int numRead=0;
    while((numRead=reader.read(buf)) != -1){
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
        buf = new char[1024];
    }
    reader.close();
    return fileData.toString();
  }
}
