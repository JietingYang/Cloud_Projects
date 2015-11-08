import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PopularityLeague extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PopularityLeague(),
				args);
		System.exit(res);
	}

	public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}

		public IntArrayWritable(Integer[] numbers) {
			super(IntWritable.class);
			IntWritable[] ints = new IntWritable[numbers.length];
			for (int i = 0; i < numbers.length; i++) {
				ints[i] = new IntWritable(numbers[i]);
			}
			set(ints);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/mp2/tmp");
		fs.delete(tmpPath, true);

		Job jobA = Job.getInstance(conf, "Popularity League");
		jobA.setOutputKeyClass(IntWritable.class);
		jobA.setOutputValueClass(IntWritable.class);

		jobA.setMapperClass(LinkCountMap.class);
		jobA.setReducerClass(LinkCountReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(PopularityLeague.class);
		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Popularity League");
		jobB.setOutputKeyClass(IntWritable.class);
		jobB.setOutputValueClass(IntWritable.class);

		jobB.setMapOutputKeyClass(NullWritable.class);
		jobB.setMapOutputValueClass(IntArrayWritable.class);

		jobB.setMapperClass(LeagueLinksMap.class);
		jobB.setReducerClass(LeagueLinksReduce.class);
		jobB.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobB, tmpPath);
		FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		jobB.setOutputFormatClass(TextOutputFormat.class);

		jobB.setJarByClass(PopularityLeague.class);
		return jobB.waitForCompletion(true) ? 0 : 1;
	}

	public static class LinkCountMap extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " :");
			Integer keyInt = 0;
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
				keyInt = Integer.parseInt(nextToken);
				context.write(new IntWritable(keyInt), new IntWritable(0));
				break;
			}
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
				keyInt = Integer.parseInt(nextToken);
				context.write(new IntWritable(keyInt), new IntWritable(1));
			}
		}
	}

	public static class LinkCountReduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static String readHDFSFile(String path, Configuration conf)
			throws IOException {
		Path pt = new Path(path);
		FileSystem fs = FileSystem.get(pt.toUri(), conf);
		FSDataInputStream file = fs.open(pt);
		BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

		StringBuilder everything = new StringBuilder();
		String line;
		while ((line = buffIn.readLine()) != null) {
			everything.append(line);
			everything.append("\n");
		}
		return everything.toString();
	}

	public static class LeagueLinksMap extends
			Mapper<Text, Text, NullWritable, IntArrayWritable> {
		private TreeSet<Pair<Integer, Integer>> countToLeagueMap = new TreeSet<Pair<Integer, Integer>>();
		List<Integer> league = new ArrayList<Integer>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String leaguePath = conf.get("league");
			List<String> leagueArray = Arrays.asList(readHDFSFile(leaguePath,
					conf).split("\n"));
			for (String i : leagueArray) {
				this.league.add(Integer.parseInt(i));
			}

		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Integer link = Integer.parseInt(value.toString());
			Integer page = Integer.parseInt(key.toString());
			if (league.contains(page))
				countToLeagueMap.add(new Pair<Integer, Integer>(link, page));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Pair<Integer, Integer> item : countToLeagueMap) {
				Integer[] integers = { item.second, item.first };
				IntArrayWritable val = new IntArrayWritable(integers);
				context.write(NullWritable.get(), val);
			}
		}
	}

	public static class LeagueLinksReduce extends
			Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
		private TreeSet<Pair<Integer, Integer>> pageToLinkMap = new TreeSet<Pair<Integer, Integer>>();
		private TreeSet<Pair<Integer, Integer>> linkToPageMap = new TreeSet<Pair<Integer, Integer>>();
		List<Integer> league = new ArrayList<Integer>();;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String leaguePath = conf.get("league");
			List<String> leagueArray = Arrays.asList(readHDFSFile(leaguePath,
					conf).split("\n"));
			for (String i : leagueArray) {
				league.add(Integer.parseInt(i));
			}
		}

		@Override
		public void reduce(NullWritable key, Iterable<IntArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntArrayWritable val : values) {
				IntWritable[] valueArray = (IntWritable[]) val.toArray();
				IntWritable page = valueArray[0];
				IntWritable link = valueArray[1];
				linkToPageMap.add(new Pair<Integer, Integer>(link.get(), page
						.get()));
				pageToLinkMap.add(new Pair<Integer, Integer>(page.get(), link
						.get()));
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Integer index = 0;
			ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();
			Integer currentIndex=0;
			Integer currentLink=0;
			for (Pair<Integer, Integer> item : linkToPageMap) {
				// IntWritable rank=new IntWritable(index);
				if(item.first==currentLink){
				map.put(item.second, currentIndex);
				currentIndex=index;
				currentLink=item.first;
				}else{
					map.put(item.second, index);
					currentIndex=index;
					currentLink=item.first;
				}
				// context.write(new IntWritable(item.second),rank);
				index++;
			}
			List<Integer> list=new ArrayList<Integer>();
			for (Pair<Integer, Integer> item : pageToLinkMap) {
				// IntWritable rank=new IntWritable(index);
				list.add(item.first);
				
			}
			
			for(int i=list.size()-1;i>=0;i--){
				Integer j=list.get(i);
				if (map.containsKey(j)) {
					IntWritable rank = new IntWritable(map.get(j));
					context.write(new IntWritable(j), rank);
				}
			}
			for (Map.Entry<Integer, Integer> i : map.entrySet()) {
				// context.write(new IntWritable(0),new IntWritable(0));
			}
			for (Integer i : league) {
				if (map.containsKey(i)) {
					IntWritable rank = new IntWritable(map.get(i));
					// IntWritable rank=new IntWritable(0);
//					context.write(new IntWritable(i), rank);
				}
			}
		}
	}
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}
