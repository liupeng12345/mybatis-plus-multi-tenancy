package com.convertlab.mybatisplusmultitenancy.mapper;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.ResultMap;
import java.util.Collection;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.convertlab.mybatisplusmultitenancy.Message;
import com.convertlab.mybatisplusmultitenancy.User;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author miemie
 * @since 2018-08-12
 */
public interface UserMapper extends BaseMapper<User> {
    List<Message> getAllMsg(@Param("name") String name);

    int insertBatch(@Param("userCollection") Collection<User> userCollection);

}
