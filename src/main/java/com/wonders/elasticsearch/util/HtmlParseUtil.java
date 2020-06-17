package com.wonders.elasticsearch.util;

import com.wonders.elasticsearch.entity.Goods;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author qiu
 * @date 2020-06-17 10:55
 */
@Component
public class HtmlParseUtil {

    public  List<Goods> getGoodsListFromJd(String keyword,Integer page)  {
        try {
            //解析网页
            String url = "https://search.jd.com/Search?keyword=" + keyword + "&enc=utf-8&page="+page;
            Document document = Jsoup.parse(new URL(url), 200000);
            Element element = document.getElementById("J_goodsList");
            Elements elements = element.getElementsByTag("li");

            List<Goods> list = new ArrayList<>();
            for (Element el : elements) {

                String sku = el.attributes().get("data-sku");

                String img = el.getElementsByTag("img").eq(0).attr("src");

                String imgLazy = el.getElementsByTag("img").eq(0).attr("source-data-lazy-img");

                String price = el.getElementsByClass("p-price").eq(0).text();

                String title = el.getElementsByClass("p-name").eq(0).text();

                String shop = el.getElementsByClass("J_im_icon").eq(0).text();

                Goods goods = new Goods();
                goods.setSku(sku);
                goods.setTitle(title);
                goods.setImg(img);
                goods.setImgLazy(imgLazy);
                goods.setPrice(price);
                goods.setShop(shop);
                list.add(goods);
            }
            return list;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

}
