package com.wonders.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author qiu
 * @date 2020-06-16 17:57
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private String username;
    private String sex;
    private Integer age;

}
