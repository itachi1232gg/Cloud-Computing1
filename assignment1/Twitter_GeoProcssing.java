import java.io.BufferedReader;
import mpi.*;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class Twitter_GeoProcssing {
    
    static int block_size = 16;
    static String twitter_path = "smallTwitter.json";
    static String a1 = ".*\"coordinates\":\\[144\\.[78]\\d(?<!8[5-9])[0-9]*,-37\\.[56]\\d(?<!6[5-9])[0-9]*\\].*";
    static String b1 = ".*\"coordinates\":\\[144\\.[78]\\d(?<!8[5-9])[0-9]*,-37\\.[67]\\d(?<!6[0-4])[0-9]*\\].*";
    static String c1 = ".*\"coordinates\":\\[144\\.[78]\\d(?<!8[5-9])[0-9]*,-37\\.[89]\\d(?<!9[5-9])[0-9]*\\].*";
    
    static String a2 = ".*\"coordinates\":\\[144\\.[89]\\d(?<!8[0-4])[0-9]*,-37\\.[56]\\d(?<!6[5-9])[0-9]*\\].*";
    static String b2 = ".*\"coordinates\":\\[144\\.[89]\\d(?<!8[0-4])[0-9]*,-37\\.[67]\\d(?<!6[0-4])[0-9]*\\].*";
    static String c2 = ".*\"coordinates\":\\[144\\.[89]\\d(?<!8[0-4])[0-9]*,-37\\.[89]\\d(?<!9[5-9])[0-9]*\\].*";
    
    static String a3 = ".*\"coordinates\":\\[145\\.[01]\\d(?<!1[5-9])[0-9]*,-37\\.[56]\\d(?<!6[5-9])[0-9]*\\].*";
    static String b3 = ".*\"coordinates\":\\[145\\.[01]\\d(?<!1[5-9])[0-9]*,-37\\.[67]\\d(?<!6[0-4])[0-9]*\\].*";
    static String c3 = ".*\"coordinates\":\\[145\\.[01]\\d(?<!1[5-9])[0-9]*,-37\\.[89]\\d(?<!9[5-9])[0-9]*\\].*";
    static String d3 = ".*\"coordinates\":\\[145\\.[01]\\d(?<!1[5-9])[0-9]*,-3(7\\.9|8\\.0)\\d(?<!9[0-4])[0-9]*\\].*";
    
    static String a4 = ".*\"coordinates\":\\[145\\.[12]\\d(?<!1[0-4])[0-9]*,-37\\.[56]\\d(?<!6[5-9])[0-9]*\\].*";
    static String b4 = ".*\"coordinates\":\\[145\\.[12]\\d(?<!1[0-4])[0-9]*,-37\\.[67]\\d(?<!6[0-4])[0-9]*\\].*";
    static String c4 = ".*\"coordinates\":\\[145\\.[12]\\d(?<!1[0-4])[0-9]*,-37\\.[89]\\d(?<!9[5-9])[0-9]*\\].*";
    static String d4 = ".*\"coordinates\":\\[145\\.[12]\\d(?<!1[0-4])[0-9]*,-3(7\\.9|8\\.0)\\d(?<!9[0-4])[0-9]*\\].*";
    
    static String c5 = ".*\"coordinates\":\\[145\\.[34]\\d(?<!4[5-9])[0-9]*,-37\\.[89]\\d(?<!9[5-9])[0-9]*\\].*";
    static String d5 = ".*\"coordinates\":\\[145\\.[34]\\d(?<!4[5-9])[0-9]*,-3(7\\.9|8\\.0)\\d(?<!9[0-4])[0-9]*\\].*";
    
    static String areas = ".*\"coordinates\":\\[14[45]\\.\\d(((?<!4\\.)[0-6])|((?<!5\\.)[5-9]))[0-9]+,-3[78]\\.\\d(((?<!7\\.)[0-4])|((?<!8\\.)[1-9]))[0-9]+\\].*";
    //144.700000 - 145.4500000
    //-37.500000 - -38.1000000
    
    static String[] patterns = {
    a1,b1,c1,a2,b2,c2,a3,b3,c3,d3,a4,b4,c4,d4,c5,d5
    };
    static String[]  block_list = {
    "A1","B1","C1","A2","B2","C2","A3","B3","C3","D3","A4","B4","C4","D4","C5","D5"
    };
    static String[] row_list ={
    "ROW-A","ROW-B","ROW-C","ROW-D"
    };
    static String[] column_list = {
    "COLUMN-1","COLUMN-2","COLUMN-3","COLUMN-4","COLUMN-5"
    };
    
    public static void main(String args[]) throws Exception {
        
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        long start = System.currentTimeMillis();
        
        if (size == 1) {// if only 1 process, master does all the jobs
            //			ArrayList<String> block_id;
            //			block_id = read_melbGrid(gird_path);
            int[] count = read_process_twitter();
            
            merge_sort_print(count,block_list);
            get_row_column(count);
            long end = System.currentTimeMillis();
            System.out.println("process time： "+(end-start)+"ms");
            
        } else {
            if (rank == 0) {// master read data and send to slaves and process
                // the data from slaves
                
                
                int[] total_count = new int[block_size];
                int[] count = new int[block_size];
                
                try {
                    String tweet = null;
                    BufferedReader bufferedReader;
                    int i = 0;
                    int x = size - 1;
                    bufferedReader = new BufferedReader(new FileReader(twitter_path));
                    
                    while ((tweet = bufferedReader.readLine()) != null) {
                        // System.out.println(tweet);
                        if (tweet.length() > 2000) {
                            MPI.COMM_WORLD.Send(tweet.substring(0, 2000).toCharArray(), 0, 2000, MPI.CHAR, (i % x) + 1,
                                                0);
                        } else {
                            MPI.COMM_WORLD.Send(tweet.toCharArray(), 0, tweet.length(), MPI.CHAR, (i % x) + 1, 0);
                        }
                        i++;
                    }
                    bufferedReader.close();
                    String end = "eof";
                    for (int j = 1; j < size; j++) {// tell slaves to stop
                        
                        MPI.COMM_WORLD.Send(end.toCharArray(), 0, end.length(), MPI.CHAR, j, 0);
                        MPI.COMM_WORLD.Recv(count, 0, block_size, MPI.INT, j, 0);
                        for (int y = 0; y < block_size; y++){
                            total_count[y] += count[y];
                        }
                    }
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                merge_sort_print(total_count, block_list);
                get_row_column(total_count);
                
                long end = System.currentTimeMillis();
                System.out.println("process time： "+(end-start)+"ms");
                
            } else {// slaves process data and finally send results back
                int[] count = new int[block_size];
                
                while (true) {
                    char[] chars = new char[2000];
                    MPI.COMM_WORLD.Recv(chars, 0, 2000, MPI.CHAR, 0, 0);
                    //					System.out.println("slaves id = " + rank + "receives" + String.valueOf(chars));
                    String tweet = new String(chars).trim();
                    //					System.out.println(tweet);
                    if(Pattern.matches(areas, tweet)){
                        for (int x = 0; x < block_size; x++) {
                            if (Pattern.matches(patterns[x], tweet)) {
                                count[x]++;
                            }
                        }
                    }
                    
                    if (tweet.equals("eof")) {//quit loop
                        //						for(int num : count){
                        //							System.out.println(num);
                        //						}
                        MPI.COMM_WORLD.Send(count, 0, block_size, MPI.INT, 0, 0);
                        break;
                    }
                    
                }
                // System.out.println(rank);
                //				System.out.println(rank + " out!!");
            }
        }
        MPI.Finalize();
        
        
        
    }
    
    public static void get_row_column(int[] count){
        int[] column_count = {
            (count[0]+count[1]+count[2]),
            (count[3]+count[4]+count[5]),
            (count[6]+count[7]+count[8]+count[9]),
            (count[10]+count[11]+count[12]+count[13]),
            (count[14]+count[15])};
        int[] row_count = {
            (count[0]+count[3]+count[6]+count[10]),
            (count[1]+count[4]+count[7]+count[11]),
            (count[2]+count[5]+count[8]+count[12]+count[14]),
            (count[9]+count[13]+count[15])
        };
        merge_sort_print(row_count, row_list);
        merge_sort_print(column_count, column_list);
    }
    
    public static void merge_sort_print(int[] count, String[] id){
        
        int length = count.length;
        Map<String, Integer> results = new HashMap<>(length);
        for (int i = 0; i < length; i++) {
            results.put(id[i], count[i]);
        }
        List<Map.Entry<String, Integer>> list = new ArrayList<>();
        list.addAll(results.entrySet());
        ValueComparator vc = new ValueComparator();
        Collections.sort(list, vc);
        System.out.println("***********************************");
        System.out.println(list);
        
        
    }
    
    
    public static int[] read_process_twitter() {
        BufferedReader brname;
        
        int[] count = new int[block_size];
        
        try {
            brname = new BufferedReader(new FileReader(twitter_path));
            String tweets = null;
            while ((tweets = brname.readLine()) != null) {
                
                if(Pattern.matches(areas, tweets)){
                    for (int x = 0; x < block_size; x++) {
                        if (Pattern.matches(patterns[x], tweets)) {
                            count[x]++;
                        }
                    }
                }
                
            }
            brname.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return count;
    }
    
    private static class ValueComparator implements Comparator<Map.Entry<String, Integer>> {
        public int compare(Map.Entry<String, Integer> m, Map.Entry<String, Integer> n) {
            return n.getValue() - m.getValue();
        }
    }
    
}
