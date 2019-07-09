package crawler_naver_news;

// jsoup
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

// regex
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// data structure
import java.util.List;
import java.util.ArrayList;

// csv
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.File;
import com.opencsv.*;

public class Slave implements Runnable  {
    Scheduler scheduler;
    String href;
    String file;

    public Slave(Scheduler scheduler, String href){
        this.scheduler = scheduler;
        this.href = href;
        this.file = "D:\\news.csv";
    }

    public boolean news_check(String url){
        Pattern p1 = Pattern.compile("read.nhn");
        Matcher m1 = p1.matcher(url);
        Pattern p2 = Pattern.compile("&oid=\\d{1,100}&aid=\\d{1,100}");
        Matcher m2 = p2.matcher(url);
        return m1.find() && m2.find();
    }



    public void news_parser(String url){
        boolean flag = true;
        BufferedReader br = null;
        String oid = null;
        String aid = null;

        // read csv, find oid & aid
        try{
            // get oid, aid
            Pattern p = Pattern.compile("&oid=(\\d{1,100})&aid=(\\d{1,100})");
            Matcher m = p.matcher(url);
            if(m.find()){
                oid = m.group(1);
                aid = m.group(2);
            }
        
            br = Files.newBufferedReader(Paths.get(this.file));
            Charset.forName("UTF-8");
            String line = "";
               
            while((line = br.readLine()) != null){
                List<String> tmpList = new ArrayList<String>();
                String array[] = line.split(",");
                if(array[0].equals(oid) && array[1].equals(aid)){
                    flag = false;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(br != null){
                    br.close();
                }
            }catch(IOException e){
                e.printStackTrace();
            }
        }

        if(flag){
            try{
                System.out.println("access into flag : " + url);
                Document doc = Jsoup.connect(url).get();
                String syn = null;

                // title
                syn = "#articleTitle";
                Elements titles = doc.select(syn);
                String title = titles.first().toString();

                // contents
                syn = "#articleBodyContents";
                Elements contents = doc.select(syn);
                String content = (contents.first().toString());

                Pattern p = Pattern.compile("\n");
                Matcher m = p.matcher(content);
                content = m.replaceAll(" ");

                // write
                CSVWriter writer = new CSVWriter(new FileWriter(this.file, true), ',');
                String[] write_contents = {oid, aid, title, content};
                writer.writeNext(write_contents);
                writer.close();
                
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }

        System.out.println("write success");
    }

    public void run() {
        if(news_check(this.href)){
            System.out.println("this is news : " + this.href);
            news_parser(this.href);
            return;
        }

        try{
            Document doc = Jsoup.connect(this.href).get();
            Elements links = doc.select("a[href]");

            List<String> news_list = new ArrayList<String>();

            for(Element link : links){
                String href = link.attr("href");
                if(!href.equals("")){
                    href = Scheduler.parsing_url(href, this.href);
                    this.scheduler.q.add(href);
                    System.out.println("give href to scheduler : " + href);
                }
                else{
                    // System.out.println(link.attr("href"));
                }
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
