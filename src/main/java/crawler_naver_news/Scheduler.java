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

public class Scheduler {
    Queue<String> q;
    String SEED = "https://news.naver.com";

    public Scheduler(){
        this.q = new LinkedList<String>();
    }

    // 잘못된 url일 경우 null 반환.
    public String parsing_url(String url, String now_url){
        Pattern p = null;
        String result = url;

        // 도메인을 가진 경우 
        p = Pattern.compile("^"+SEED);
        if(p.matcher(url).find()){

            // 가장 많이본 뉴스 처리 
            p = Pattern.compile("&date=\\d{1,100}&type=\\d{1,100}&rankingSeq=\\d{1,100}&rankingSectionId=\\d{1,100}");
            Matcher m = p.matcher(url);
            if(m.find()){
                result = m.replaceAll("");
            }
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
                href = parsing_url(href, SEED);
                if(href != null){
                    q.add(href);
                }
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void run() {
        get_seed();
        for(String s : this.q){
            System.out.println(s);
        }
    }
}
