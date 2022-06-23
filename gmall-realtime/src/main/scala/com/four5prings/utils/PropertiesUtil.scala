package com.four5prings.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @ClassName PropertiesUtil
 * @Description 创建配置文件对象
 * @Author Four5prings
 * @Date 2022/6/23 14:47
 */
object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    //创建properties对象
    val properties = new Properties()

    /**
     * 加载传入的配置文件，并设置编码格式为utf-8
     *  load方法加载需要传入一个流，那么创建一个流，使用反射，用当前流找到类加载器找到流，传入配置文件名
     */
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream
    (propertiesName),"UTF-8"))

    //返回该对象
    properties
  }
}
