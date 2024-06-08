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
package org.apache.ibatis.executor.keygen;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
/**
 * JDBC3键值生成器,核心是使用JDBC3的Statement.getGeneratedKeys
 * 
 */
public class Jdbc3KeyGenerator implements KeyGenerator {

  @Override
  public void processBefore(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {
    // do nothing
  }

  @Override
  public void processAfter(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {
    List<Object> parameters = new ArrayList<Object>();
    parameters.add(parameter);
    processBatch(ms, stmt, parameters);
  }

  //批处理
  public void processBatch(MappedStatement ms, Statement stmt, List<Object> parameters) {
    ResultSet rs = null;
    try {
      //核心是使用JDBC3的Statement.getGeneratedKeys
      // 生成的主键值
      // jdbc的getGeneratedKeys方法，用于获取数据库自动生成的主键值
      rs = stmt.getGeneratedKeys();
      final Configuration configuration = ms.getConfiguration();
      final TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
      // 主键需要赋值的属性。
      // 可以有多个，用逗号隔开
      // entity.id,entity1.id
      final String[] keyProperties = ms.getKeyProperties();
      // 表的元数据
      final ResultSetMetaData rsmd = rs.getMetaData();
      TypeHandler<?>[] typeHandlers = null;
      // keyProperties是一个数据没啥用呢。因为只有一个主键。这里走不进去了
      if (keyProperties != null && rsmd.getColumnCount() >= keyProperties.length) {
        // 可有多个参数。比如一个入参数也可以有多个参数
        // entity, param1
        // 如果是entity一般就是一个参数
        for (Object parameter : parameters) {
          // there should be one row for each statement (also one for each parameter)
          if (!rs.next()) {
            break;
          }
          // 创建元对象
          final MetaObject metaParam = configuration.newMetaObject(parameter);
          if (typeHandlers == null) {
            //先取得类型处理器
            typeHandlers = getTypeHandlers(typeHandlerRegistry, metaParam, keyProperties);
          }
          // 填充键值
          // 通过类型处理器，将生成的主键值填充到参数对象中
          populateKeys(rs, metaParam, keyProperties, typeHandlers);
        }
      }
    } catch (Exception e) {
      throw new ExecutorException("Error getting generated key or setting result to parameter object. Cause: " + e, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  private TypeHandler<?>[] getTypeHandlers(TypeHandlerRegistry typeHandlerRegistry, MetaObject metaParam, String[] keyProperties) {
    TypeHandler<?>[] typeHandlers = new TypeHandler<?>[keyProperties.length];
    for (int i = 0; i < keyProperties.length; i++) {
      if (metaParam.hasSetter(keyProperties[i])) {
        Class<?> keyPropertyType = metaParam.getSetterType(keyProperties[i]);
        TypeHandler<?> th = typeHandlerRegistry.getTypeHandler(keyPropertyType);
        typeHandlers[i] = th;
      }
    }
    return typeHandlers;
  }

  /**
   * 填充主键
   * @param rs
   * @param metaParam
   * @param keyProperties
   * @param typeHandlers
   * @throws SQLException
   */
  private void populateKeys(ResultSet rs, MetaObject metaParam, String[] keyProperties, TypeHandler<?>[] typeHandlers) throws SQLException {
    for (int i = 0; i < keyProperties.length; i++) {
      TypeHandler<?> th = typeHandlers[i];
      if (th != null) {
        // 获取返回结果，其实就是id的值
        Object value = th.getResult(rs, i + 1);
        // 通过typeHandler进行id的赋值
        metaParam.setValue(keyProperties[i], value);
      }
    }
  }

}
