import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StationMapper1
  extends Mapper<LongWritable, Text, Text, IntWritable>{

  private static final int MISSING = 9999;
  int airTemperature;
  String e;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String year = " ";					//line.substring(15, 19);
     int stationID1, SID2;
try{
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    }else{
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
}
catch (NumberFormatException E){
	
	e = E.toString();
//airTemperature = 00000;
	/*stationID = Integer.parseInt(line.substring(5,10));
	ArrayList<Integer> list = new ArrayList<Integer>(); 
	list.add(stationID);
SID = sort(list);*/

}
    //String quality = line.substring(92, 93);
 if (airTemperature == MISSING && e != null ) {
    ArrayList<Integer> list2 = new ArrayList<Integer>();  
    stationID1 = Integer.parseInt(line.substring(5,10)); 
list2.add(stationID1);
SID2 = sort(list2);
context.write(new Text(year), new IntWritable(SID2));
  }
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
