package com.convertlab.mybatisplusmultitenancy.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.convertlab.mybatisplusmultitenancy.Message;
import com.convertlab.mybatisplusmultitenancy.User;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface MessageMapper extends BaseMapper<Message> {
}
