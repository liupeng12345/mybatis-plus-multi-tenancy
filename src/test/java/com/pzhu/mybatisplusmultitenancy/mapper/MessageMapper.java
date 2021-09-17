package com.pzhu.mybatisplusmultitenancy.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pzhu.mybatisplusmultitenancy.Message;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface MessageMapper extends BaseMapper<Message> {
}
