package com.cmbc.kafkatest.custompartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserServiceImpl implements IUserService{
    //Pairs of UserName and id
    private Map<String,Integer> userMap;
    public UserServiceImpl(){
        userMap = new HashMap<String, Integer>();
        userMap.put("tom",1);  //构建一个映射
        userMap.put("bob",2);
    }
    public List<String> findAllUsers() {
        return new ArrayList<String>(userMap.keySet());
    }

    public Integer findUserId(String userName) {
        return userMap.get(userName);
    }
}
