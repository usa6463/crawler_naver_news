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
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

public class Crawler {
    Queue<String> q;
    String SEED = "https://news.naver.com";

    public Crawler(){
        this.q = new LinkedList<String>();
    }

    // 잘못된 url일 경우 null 반환.
    public String parsing_url(String url, String now_url){
        Pattern p = null;
        String result = url;

        // 도메인을 가진 경우 
        p = Pattern.compile("^"+SEED);
        if(p.matcher(url).find()){
            
        }
        // 도메인을 가지지 않은 경우 
        else{
            Pattern p1 = Pattern.compile("^#");
            Pattern p2 = Pattern.compile("^/");
            Pattern p3 = Pattern.compile("^?");
            if(p1.matcher(url).find()){
                result = now_url + url;
            }
            else if(p2.matcher(url).find()){
                result = SEED + url;
            }
            else if(p3.matcher(url).find()){
                p = Pattern.compile("?.*");
                Matcher m = p.matcher(now_url);
                result = m.replaceAll(url);
            }
            else{
                System.out.println("Error : " + url);
                result = null;
            }
        }
        return result;
    }

    public void get_seed(){
        try {
            Document doc = Jsoup.connect(SEED).get();
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

    public void run() {
        

        

       
    }
}
