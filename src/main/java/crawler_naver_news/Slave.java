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
        Pattern p = Pattern.compile("&oid=\\d{1,100}&aid=\\d{1,100}");
        Matcher m = p.matcher(url);
        return m.find();
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
            oid = m.group(1);
            aid = m.group(2);
        
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
                Document doc = Jsoup.connect(url).get();

                // title
                Elements e = doc.getElementsByClass("tts_head");
                String title = e.first().toString();

                // contents
                e = doc.getElementsByClass("_article_body_contents");
                String contents = e.first().toString();

                // BufferedWriter bufWriter = null;
                // bufWriter = Files.newBufferedWriter(Paths.get(this.file),Charset.forName("UTF-8"));
                FileWriter pw = new FileWriter(this.file,true); 
                pw.append(oid+","+aid+","+title+","+contents+"\n");
                
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }
            
        }
    }

    public void run() {
        if(news_check(this.href)){
            System.out.println("this is news : " + this.href);
            news_parser(this.href);
        }

        try{
            Document doc = Jsoup.connect(this.href).get();
            Elements links = doc.select("a[href]");

            List<String> news_list = new ArrayList<String>();

            for(Element link : links){
                String href = link.attr("href");
                href = Scheduler.parsing_url(href, this.href);
                if(!href.equals("")){
                    this.scheduler.q.add(href);
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
