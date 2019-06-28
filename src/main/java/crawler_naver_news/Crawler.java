package crawler_naver_news;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

public class Crawler {
    public void run() {
        String url = "https://news.naver.com/";

        try {
            Document doc = Jsoup.connect(url).get();
            Elements links = doc.select("a[href]");

            for(Element link : links){
                System.out.println(link.attr("href"));
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
