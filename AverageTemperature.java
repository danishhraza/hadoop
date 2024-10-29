import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTemperature {

    // Mapper class
    public static class TemperatureMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

        private final static IntWritable year = new IntWritable();
        private final static DoubleWritable temperature = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Extract year and temperature
            String yearString = line.substring(15, 19);   // year is at position (15-19)
            String tempString = line.substring(87, 92);   // temperature is at position (87-92)

            // Convert to integer and double
            int yearValue = Integer.parseInt(yearString);
            double tempValue = Double.parseDouble(tempString) / 10.0;

            // Filter invalid temperatures
            if (tempValue != 999.9) {
                year.set(yearValue); // Set the year
                temperature.set(tempValue); // Set the temperature
                context.write(year, temperature); // Emit (year, temperature)
            }
        }
    }

    // Reducer class
    public static class TemperatureReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double sum = 0.0;

            // Sum all temperatures and count the records
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            // Calculate average temperature
            double average = sum / count;
            context.write(key, new DoubleWritable(average)); // Emit (year, average temperature)
        }
    }

    // Main method to configure and run the job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average temperature");

        job.setJarByClass(AverageTemperature.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureReducer.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
