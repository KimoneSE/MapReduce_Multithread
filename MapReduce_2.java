import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * created by Kimone
 * date 2020/8/25
 */
public class MapReduce_2 {

    private int num = 0;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        MapReduce_2 mapReduce_2 = new MapReduce_2();

        String filePath = "E:\\hamlet.txt";

        List<String> content = mapReduce_2.readFile(filePath);

        Map<String,Integer> reduceRes = mapReduce_2.mapReduce(content);

        mapReduce_2.print(reduceRes);

        long endTime = System.currentTimeMillis();

        System.out.println("共花费时长"+(endTime-startTime)+"ms");

    }

    private List<String> readFile(String filePath) {
        ArrayList<String> arrayList = new ArrayList<>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(new File(filePath)));
            String line;
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

    private Map<String, Integer> mapReduce(List<String> content){
        List<Data> listAfterMapping = mapping(content);

        Map<String,List<String>> shuffleRes = shuffling(listAfterMapping);

        Map<String,Integer> reduceRes = reducing(shuffleRes);

        return reduceRes;
    }


    private List<Data> mapping(List<String> content) {
        List<Data> listAfterMapping = Collections.synchronizedList(new ArrayList<Data>());
        int mapTaskNum = 3;//map过程的线程数
        final CountDownLatch latchMap = new CountDownLatch(mapTaskNum);
        ExecutorService executorService = Executors.newFixedThreadPool(mapTaskNum);

        try {
            for(int i=0;i<mapTaskNum;i++) {
                List<String> part = content.subList(i*num/mapTaskNum,(i+1)*num/mapTaskNum);
                executorService.execute(()->{
                    List<String> words = processSplitting(part);
                    List<Data> data = processMapping(words);
                    listAfterMapping.addAll(data);
                    latchMap.countDown();
                });
            }
            latchMap.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
        return listAfterMapping;
    }

    private Map<String,List<String>> shuffling(List<Data> dataList) {
        Collections.sort(dataList, Comparator.comparing(Data::getKey));
        int size = dataList.size();
        Map<String, List<String>> mapRes = new ConcurrentHashMap<>();
        int shuffleTaskNum = 3;
        final CountDownLatch latchShuffle = new CountDownLatch(shuffleTaskNum);
        ExecutorService executorService = Executors.newFixedThreadPool(shuffleTaskNum);
        try {
            for(int i=0;i<shuffleTaskNum;i++) {
                List<Data> part = dataList.subList(i*size/shuffleTaskNum,(i+1)*size/shuffleTaskNum);
                Map<String,List<String>> map = new HashMap<>();
                executorService.execute(()->{
                    for(Data data: part) {
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
                    mapRes.putAll(map);
                    latchShuffle.countDown();
                });
            }
            latchShuffle.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
        return mapRes;
    }

    private Map<String,Integer> reducing(Map<String,List<String>> map) {
        Map<String,Integer> mapRes = new ConcurrentSkipListMap<>();
        //reduce过程的线程数
        int reduceTaskNum = 3;
        final CountDownLatch latchReduce = new CountDownLatch(reduceTaskNum);
        ExecutorService executorService = Executors.newFixedThreadPool(reduceTaskNum);
        List<String> words = new ArrayList<>(map.keySet());
        int size = words.size();
        try {
            for(int i=0;i<reduceTaskNum;i++) {
                List<String> wordPart = words.subList(i*size/reduceTaskNum,(i+1)*size/reduceTaskNum);
                Map<String,Integer> tmp = new HashMap<>();
                executorService.execute(()->{
                    for(String key:wordPart) {
                        tmp.put(key,map.get(key).size());
                    }
                    mapRes.putAll(tmp);
                    latchReduce.countDown();
                });
            }
            latchReduce.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
        return mapRes;
    }

    private List<String> processSplitting(List<String> list) {
        ArrayList<String> res = new ArrayList<>();
        for(String line: list) {
            res.addAll(Arrays.asList(line.split(" ")));
        }
        return res;
    }

    private List<Data> processMapping(List<String> list) {
        List<Data> res = new ArrayList<>();
        for(String str:list) {
            if(str!=null&&str.trim().length()>0) {
                Data data = new Data(str.trim(),"1");
                res.add(data);
            }

        }
        return res;
    }

    private void print(Map<String,Integer> map) {
        FileOutputStream fileOutputStream;
        File file = new File("E:\\map_reduce_result.txt");
        if(!file.exists()){
            try {
                file.createNewFile();
                fileOutputStream = new FileOutputStream(file);
                for(String key:map.keySet()) {
                    String s = key+": "+map.get(key)+"\r\n";
                    fileOutputStream.write(s.getBytes());
//                    System.out.println(key+": "+map.get(key));
                }
                fileOutputStream.flush();
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

class Data{
    private String key;
    private String value;

    String getKey() {
        return key;
    }

    void setKey(String key) {
        this.key = key;
    }

    String getValue() {
        return value;
    }

    void setValue(String value) {
        this.value = value;
    }

    Data(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Data{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}