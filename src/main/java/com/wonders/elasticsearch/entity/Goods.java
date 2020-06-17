package com.wonders.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;

/**
 * @author qiu
 * @date 2020-06-17 10:57
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="jd_goods")
public class Goods {

    @Id
    @GeneratedValue(generator="system-uuid")
    @GenericGenerator(name="system-uuid", strategy = "uuid")
    protected String id;

    @Column(name="sku")
    private String sku;

    @Column(name="title")
    private String title;

    @Column(name="img")
    private String img;

    @Column(name="img_lazy")
    private String imgLazy;

    @Column(name="price")
    private String price;

    @Column(name="shop")
    private String shop;

    @Override
    public String toString() {
        return "Goods{" +
                "id='" + id + '\'' +
                ", sku='" + sku + '\'' +
                ", title='" + title + '\'' +
                ", img='" + img + '\'' +
                ", imgLazy='" + imgLazy + '\'' +
                ", price='" + price + '\'' +
                ", shop='" + shop + '\'' +
                '}';
    }
}
