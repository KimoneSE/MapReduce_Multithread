import java.io.*;
import java.util.*;

/**
 * created by Kimone
 * date 2020/8/24
 */
public class MapReduce_SingleThread {
    static int num = 0;
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String filePath = "E:\\hamlet.txt";

        List<String> list = getContent(filePath);

        List<String> listSplit1 = processSplitting(list.subList(0, num/3));
        List<String> listSplit2 = processSplitting(list.subList(num/3,2*num/3));
        List<String> listSplit3 = processSplitting(list.subList(2*num/3,num));
        List<Data> listMapping1 = processMapping(listSplit1);
        List<Data> listMapping2 = processMapping(listSplit2);
        List<Data> listMapping3 = processMapping(listSplit3);

        Map map = processShuffle(listMapping1, listMapping2, listMapping3);
        Map<String,Integer> res = processReduce(map);

        FileOutputStream fileOutputStream = null;
        File file = new File("E:\\res.txt");
        if(!file.exists()){
            try {
                file.createNewFile();
                fileOutputStream = new FileOutputStream(file);
                for(String key:res.keySet()) {
                    String s = key+": "+res.get(key)+"\r\n";
                    fileOutputStream.write(s.getBytes());
                }
                fileOutputStream.flush();
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.println("共花费时长"+(endTime-startTime)+"ms");
    }

    private static void print(List<String> list) {
        for(String str:list){
            System.out.println(str);
        }
    }
    private static List<String> processSplitting(List<String> list) {
        ArrayList<String> res = new ArrayList<>();
        for(String line: list) {
            res.addAll(Arrays.asList(line.split(" ")));
        }
        return res;
    }

    private static List<Data> processMapping(List<String> list) {
        List<Data> res = new ArrayList<>();
        for(String str:list) {
            if(str!=null&&str.trim().length()>0) {
                Data data = new Data(str.trim(),"1");
                res.add(data);
            }

        }
        return res;
    }

    private static Map<String, List<String>> processShuffle(List<Data>... datas) {
        TreeMap<String, List<String>> map = new TreeMap<>();
        for(List<Data> dataList:datas) {
            for(Data data: dataList) {
                String key = data.getKey();
                if(map.containsKey(key)) {
                    List<String> listV = map.get(key);
                    listV.add(data.getValue());
                    map.put(key,listV);
                }else {
                    List<String> list = new ArrayList<>();
                    list.add("1");
                    map.put(key, list);
                }
            }
        }
        return map;
    }

    private static Map<String, Integer> processReduce(Map<String,List<String>> map) {
        Map<String, Integer> resMap = new TreeMap<>();
        for(String key:map.keySet()) {
            resMap.put(key,map.get(key).size());
        }
        return resMap;
    }
    private static List<String> getContent(String filePath) {
        ArrayList<String> arrayList = new ArrayList<>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(new File(filePath)));
            String line = null;
            while ((line = br.readLine()) != null) {
                line = line.replaceAll( "[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]" , " ");
                line = line.toLowerCase();
                arrayList.add(line);
                num++;
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }


}

class Data{
    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    Data(String key, String value) {
        this.key = key;
        this.value = value;
    }

}