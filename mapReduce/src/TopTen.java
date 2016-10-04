package sics;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;
import java.util.TreeMap;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		// Generate an initial document builder for all the Map operations 
		private static DocumentBuilder builder = newDocumentBuilder();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Parse XML String into Document format, and retrieve only the <row> elements
			Document parsed = loadXMLFromString(value.toString());
			NodeList userList = parsed.getElementsByTagName("row");

			for (int i = 0; i < userList.getLength(); i++) {
				// Even though the dataset (supposes) one user per map operation, we ensure the 
				// execution with the for-loop
				Element user = (Element) userList.item(i);
				Integer reputation = Integer.parseInt(user.getAttribute("Reputation"));

				// There can be duplicates in Reputation, but we have to assume that they 
				// belong to the same user
				repToRecordMap.put(reputation, new Text(reputation + "·" + value));

				// If we have more than ten records, remove the one with the lowest reputation
				if (repToRecordMap.size() > 10) {
					repToRecordMap.pollFirstEntry();
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), new Text(t));
			}
		}

		// Factory method for creating a new DocumentBuilder instance
		private static DocumentBuilder newDocumentBuilder() {

			DocumentBuilder builder = null;

			try {

				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				builder = factory.newDocumentBuilder();

			} catch (ParserConfigurationException e) { 
				e.printStackTrace();
			}

			return builder;
		}

		// Parses the input XML string and returns a document
		private static Document loadXMLFromString(String xml) {

			Document parsed = null;

			InputSource is = new InputSource(new StringReader(xml));

			try {
				parsed = builder.parse(is);
			} catch (Exception e) { // In case of wrong format, generate an empty document
				parsed = builder.newDocument();
			}

			return parsed;
		}
	}

	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		// Overloads the comparator to order the reputations in descending order
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				
				// Split the input value to obtain the reputation and the record itself
				String[] splitData = value.toString().split("·", 2);

				// Add the record to the tree map, according to the output format of the Mapper
				// [Reputation, <row>]
				repToRecordMap.put(Integer.parseInt(splitData[0]), new Text(splitData[1]));

				// If we have more than ten records, remove the one with the
				// lowest reputation
				if (repToRecordMap.size() > 10) {
					repToRecordMap.pollFirstEntry();
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				// Output our ten records to the file system with a null key
				context.write(NullWritable.get(), new Text(t));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top ten");
		job.setJarByClass(TopTen.class);

		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Make sure only a reducer is executed
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
