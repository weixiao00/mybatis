/*
 *    Copyright 2009-2012 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.scripting.defaults;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
/**
 * 默认参数处理器
 * 
 */
public class DefaultParameterHandler implements ParameterHandler {

  private final TypeHandlerRegistry typeHandlerRegistry;

  private final MappedStatement mappedStatement;
  // 应该也是一个参数map
  // key：0,1,2,param1,param2
  // value：具体的参数值
  private final Object parameterObject;
  private BoundSql boundSql;
  private Configuration configuration;

  public DefaultParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
    this.mappedStatement = mappedStatement;
    this.configuration = mappedStatement.getConfiguration();
    this.typeHandlerRegistry = mappedStatement.getConfiguration().getTypeHandlerRegistry();
    this.parameterObject = parameterObject;
    this.boundSql = boundSql;
  }

  @Override
  public Object getParameterObject() {
    return parameterObject;
  }

  //设置参数
  @Override
  public void setParameters(PreparedStatement ps) throws SQLException {
    ErrorContext.instance().activity("setting parameters").object(mappedStatement.getParameterMap().getId());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    if (parameterMappings != null) {
      //循环设参数
      for (int i = 0; i < parameterMappings.size(); i++) {
        ParameterMapping parameterMapping = parameterMappings.get(i);
        if (parameterMapping.getMode() != ParameterMode.OUT) {
          //如果不是OUT，才设进去
          Object value;
          // java的属性名字
          // 如果只有一个参数，这里填任何值都可以映射到
          String propertyName = parameterMapping.getProperty();
          if (boundSql.hasAdditionalParameter(propertyName)) { // issue #448 ask first for additional params
            //若有额外的参数, 设为额外的参数
            value = boundSql.getAdditionalParameter(propertyName);
          } else if (parameterObject == null) {
            //若参数为null，直接设null
            value = null;
          } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
            //若参数有相应的TypeHandler，直接设object
            // 有typeHandler说明是基本数据类型，直接设进去
            // 如果只有一个参数，这里填任何值都可以映射到。除了我们entity。基本数据类型才行
            value = parameterObject;
          } else {
            //除此以外，MetaObject.getValue反射取得值设进去
            // 多个参数，这里就是map类型。mapwrapper
            // 一个entity类型的参数。就是一个beanwrapper
            MetaObject metaObject = configuration.newMetaObject(parameterObject);
            // 这里其实就是从map里取值
            // 或者是从entity里取值
            value = metaObject.getValue(propertyName);
          }
          // 这里一定不为空，没有指定会有默认值
          TypeHandler typeHandler = parameterMapping.getTypeHandler();
          JdbcType jdbcType = parameterMapping.getJdbcType();
          if (value == null && jdbcType == null) {
            //不同类型的set方法不同，所以委派给子类的setParameter方法
            jdbcType = configuration.getJdbcTypeForNull();
          }
          typeHandler.setParameter(ps, i + 1, value, jdbcType);
        }
      }
    }
  }

}
