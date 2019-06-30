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

public class Slave implements Runnable  {
    Scheduler scheduler;
    public Slave(Scheduler scheduler){
        this.scheduler = scheduler;
    }

    public void run() {
        String url = "https://news.naver.com/";

        try {
            Document doc = Jsoup.connect(url).get();
            Elements links = doc.select("a[href]");

            List<String> news_list = new ArrayList<String>();

            for(Element link : links){
                String href = link.attr("href");
                Pattern p = Pattern.compile("\\S{1,100}oid=\\d{1,100}&aid=\\d{1,100}.*");
                boolean news_check = p.matcher(href).find();

                p = Pattern.compile("^/main.*");
                boolean main_start_check = p.matcher(href).find();
                if(news_check && main_start_check){
                    href = "https://news.naver.com" + href;
                }
                if(news_check){
                    news_list.add(href);
                } 
            }

            for(String link : news_list){
                System.out.println(link);
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
