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
            System.out.println(doc);
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
