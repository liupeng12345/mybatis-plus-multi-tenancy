package com.pzhu.mybatisplusmultitenancy.mapper;
import com.pzhu.mybatisplusmultitenancy.Message;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pzhu.mybatisplusmultitenancy.User;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author miemie
 * @since 2018-08-12
 */
public interface UserMapper extends BaseMapper<User> {
    List<Message> getAllMsg(@Param("name") String name);

}
