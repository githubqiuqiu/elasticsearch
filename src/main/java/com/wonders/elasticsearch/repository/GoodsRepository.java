package com.wonders.elasticsearch.repository;

import com.wonders.elasticsearch.entity.Goods;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @author qiu
 * @date 2020-06-17 13:38
 */
@Repository
public interface GoodsRepository extends JpaRepository<Goods, String> {
}
