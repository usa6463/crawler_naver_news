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

    // if wrong url, this method will return null
    public String parsing_url(String url, String now_url){
        Pattern p = null;
        String result = url;

        // if url has domain
        p = Pattern.compile("^http");
        if(p.matcher(url).find()){

            p = Pattern.compile("://news.naver.com");
            if(!p.matcher(url).find()){
                result = "";
            }

            // most high views 
            p = Pattern.compile("&date=\\d{1,100}&type=\\d{1,100}&rankingSeq=\\d{1,100}&rankingSectionId=\\d{1,100}");
            Matcher m = p.matcher(url);
            if(m.find()){
                result = m.replaceAll("");
            }
        }
        // if url does not have domain
        else{
            Pattern p1 = Pattern.compile("^#&");
            Pattern p2 = Pattern.compile("^/");
            Pattern p3 = Pattern.compile("^?");
            if(p1.matcher(url).find()){
                result = now_url + url;
            }
            else if(p2.matcher(url).find()){
                result = SEED + url;
            }
            else if(p3.matcher(url).find()){
                p = Pattern.compile("\\?.*");
                Matcher m = p.matcher(now_url);
                result = m.replaceAll(url);
            }
            else{
                result = "";
            }
        }

        if(result.equals(now_url) || result.equals(now_url+"/") || (result+"/").equals(now_url)){
            result = "";
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
                if(!href.equals("")){
                    q.add(href);
                }
                else{
                    // System.out.println(link.attr("href"));
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
