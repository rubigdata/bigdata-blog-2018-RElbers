# Map-Reduce
Map-Reduce works by mapping the input and then reducing it. The computations are distributed over many different systems. This distribution is done by the Map-Reduce framework and is hidden to the user. The Mapper recieves a part of the input, maps it to a value and sends it and a key to the Reducer. Or rather the framework takes all the values belonging to a particular key and sends it to the Reducer. The Reducer receives these values for a certain key and combines it to produce another value. In our WordCount example the Mapper simply writes the value 1 for every word. The Reducer sums up all the 1s of a word and produces a total count for that word.   
 
## Mapper
```java
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
}
```

## Reducer
```java
public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
```

# Running Map-Reduce
First we need to format the HDFS and start the Hadoop DFS daemons and nodes.
```
bin/hdfs namenode -format
sbin/start-dfs.sh
```

We can now run a filesystem command using hdfs dfs.
```
bin/hdfs dfs -mkdir /my/new/folder
bin/hdfs dfs -put /from/local /to/hdfs
bin/hdfs dfs -get /from/hdfs /to/local
```

For hadoop and MapReduce to work properly we need to make sure the environment variable are set correctly.
```
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

Go to the docker folder. Download the script. Place the script into the input folder in hdfs.
```
cd /opt/docker/hadoop-2.7.3
wget https://raw.githubusercontent.com/rubigdata/hadoop-dockerfile/master/100.txt
bin/hdfs dfs -put 100.txt input
```

Compile WordCount.java using javac.
```
bin/hadoop com.sun.tools.javac.Main WordCount.java
```

Make a jar of the .class files.
```
jar cf wc.jar WordCount*.class
```

Remove the output folder in case it already existed.
```
bin/hdfs dfs -rm -r output
```

Run Map-Reduce using the Mapper and Reducer in wc.jar, with WordCount being the class with the main method, input being the folder containing 100.txt and output being the folder that will contain the output.
```
bin/hadoop jar wc.jar WordCount input output
```

# Processing the output  
The output is hidden deep in the bowels of the dfs. But we can run cat on the folder and pipe it to a file. Words.txt is not that large so there is nothing to worry about.

```
$ bin/hadoop dfs -cat output/* > words.txt
```

Words.txt now contains a count for every word in the script. This includes words like: 'The', 'William' and 'UTF-8'. Furthermore  Romeo and Julia come in many different shapes and sizes. Like 'Julia' and 'julia:'. We are interested in all these variants. Luckily we can use a regex and grep to help us. And then filter it to get only the numbers.
```
$ grep -i "julia" words.txt | grep -Eo "[0-9]*"  
8  
3  
1  
107  
12  
2  
1  
5  
7  
3  
2  
2  


$ grep -i "romeo" words.txt | grep -Eo "[0-9]*"  
2  
2  
163  
45  
11  
1  
15  
1  
35  
1  
8  
11  
1  
2  
2  
1  
8  
1  
1  
1  
1  
```

Now using our favourite calculator we can sum up the counts.
```
Julia:
8+3+1+107+12+2+1+5+7+3+2+2=153
Romeo:
2+2+163+45+11+1+15+1+35+1+8+11+1+2+2+1+8+1+1+1+1=313
```

# But wait, there's more
The above result is messy and requires post processing. We are only interested in the counts for Romeo and Julia, but our output file contains the counts for every word.

We can change the Mapper to better match this specific task. We will change the Mapper so that it tries to match a word with 'romeo' or 'julia' (case-insensitivly). When there is a match it emits 'romeo' or 'julia' respectivly. When there is no match, it does not emit any word. So in the final output there are only 2 counts, one for Romeo and one for Julia. 

I'm not sure if it is actually more efficient this way. The Mapper is doing more work, but the Reducer has less data to reduce because not every word is emitted. But for such a small file it is definitely more convenient. Of course, since the output only contains julia and romeo, if you want to know how much the word 'the' occurs you will need to run the job again with a different Mapper.

In conclusion, (some variant of) Romeo occurs about twice as often as Julia. And we found that out with only 1 pass over the corpus.

## Output
```
$ bin/hadoop dfs -cat output/*
julia	153
romeo	313
```

## Mapper
```java
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static Pattern ROMEO = Pattern.compile(Pattern.quote("romeo"), Pattern.CASE_INSENSITIVE);
    private final static Pattern JULIA = Pattern.compile(Pattern.quote("julia"), Pattern.CASE_INSENSITIVE);

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());

            if (ROMEO.matcher(word.toString()).find()) {
                word.set("romeo");
                context.write(word, one);
            }else if(JULIA.matcher(word.toString()).find()){
                word.set("julia");
                context.write(word, one);
            }
        }
    }
}
```

# Protip
Don't try to do this in Windows. I thought docker was supposed to make this easy, by just allowing you to load a container and use it. However I was not able to get bash (docker exec -it big_data \bin\bash) to work. Because the container is linux and the host is windows, there is a mismatch in input and I can not just use powershell to send input to the linux os. I tried using the linux subsystem for windows (Bash on Ubuntu on Windows), but docker still complained that it was not a tty. After googling I found out I can use winpty for this. However I could not get it to work. I tried MSYS with MinGW, MSYS2 and Cygwin but I could not install winpty. I tried the Babun shell for windows, which had winpty preinstalled, but that also gave me some obscure error. I gave up trying to get bash container to work in the container in windows and I installed Ubuntu on a virtual machine and worked on that.







