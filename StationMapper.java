import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StationMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature, stationID, SID;
try{
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    }else{
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
}
catch (NumberFormatException e){
//airTemperature = 00000;
	stationID = Integer.parseInt(line.substring(5,10));
	ArrayList<Integer> list = new ArrayList<Integer>(); 
	list.add(stationID);
SID = sort(list);
context.write(new Text(year), new IntWritable(SID));
}
    //String quality = line.substring(92, 93);
    //if (airTemperature != MISSING && quality.matches("[01459]")) {
      
    //}
  }
  public static int sort(List<Integer> list) {
      
      int i =0;
      Map<Integer,Integer> map = new HashMap<Integer, Integer>();
      for(int s: list)
      {
        Integer c = map.get(s);
        if(c == null) c = new Integer(0);
        c++;
        map.put(s,c);
      }
      Map.Entry<Integer,Integer> mostRepeated = null;
      for(Map.Entry<Integer, Integer> e: map.entrySet())
      {
          if(mostRepeated == null || mostRepeated.getValue()<e.getValue())
              mostRepeated = e;
          i = mostRepeated.getKey();
      }
      if(mostRepeated != null){
          //System.out.println("Most common string: " + mostRepeated.getKey());
         
      }
      //System.out.println(i);
      return i;
  }
}
