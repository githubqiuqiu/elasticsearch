package com.wonders.elasticsearch.controller;

import com.wonders.elasticsearch.service.GoodsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author qiu
 * @date 2020-06-17 13:39
 */
@RestController
public class GoodsController {
    @Autowired
    private GoodsService goodsService;

    @GetMapping("/initGoods/{keyword}")
    public void initGoods(@PathVariable("keyword") String keyword){
        try {
            goodsService.initGoods(keyword);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @GetMapping("/searchList/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String,Object>> searchList(@PathVariable("keyword")String keyword,
                                               @PathVariable("pageNo")Integer pageNo,
                                               @PathVariable("pageSize")Integer pageSize){
        try {
            return goodsService.searchList(keyword,pageNo,pageSize);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

}
