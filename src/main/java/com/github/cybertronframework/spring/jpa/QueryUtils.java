package com.github.cybertronframework.spring.jpa;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.Enumerated;
import javax.persistence.Query;
import lombok.SneakyThrows;
import org.hibernate.SQLQuery;
import org.hibernate.transform.Transformers;
import org.hibernate.type.CustomType;
import org.hibernate.type.DoubleType;
import org.hibernate.type.EnumType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.Type;
import org.hibernate.type.spi.TypeConfiguration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author dunght1
 * @version 1.0
 * @since Feb 02, 2021
 */
public class QueryUtils {

    private QueryUtils() {
    }

    public static <T> Page<T> getResultList(EntityManager entityManager, StringBuilder select, StringBuilder from, StringBuilder defaultOrder,
                                            Pageable pageable, Map<String, Object> mapParams, Map<String, List<?>> mapParamsList, Class<T> tClass) {
        Query countQuery = entityManager.createNativeQuery("SELECT COUNT(*) as total " + from.toString());
        setParameter(countQuery.unwrap(SQLQuery.class), mapParams, mapParamsList).addScalar("total", IntegerType.INSTANCE);
        Integer total = (Integer) countQuery.getSingleResult();

        String order;
        if (pageable.getSort().isSorted()) {
            order = " ORDER BY " + pageable.getSort().toString().replace(":", " ");
        } else {
            order = " ORDER BY " + defaultOrder.toString();
        }
        Query query = entityManager.createNativeQuery(select.append(from).append(order).toString());
        SQLQuery sqlQuery = setParameter(query.unwrap(SQLQuery.class), mapParams, mapParamsList);

        List<String> aliasColumnName = Arrays.stream(select.toString()
                                                           .trim()
                                                           .split("(^SELECT)|([,])"))
                                             .filter(StringUtils::hasText)
                                             .map(s -> {
                                                 String res = s.trim();
                                                 return res.replaceAll("^(.+)(?=as)as\\s(.+)$", "$2")
                                                           .replaceAll("^([^\\.]+)(?=\\.)\\.(.+)$", "$2");
                                             }).collect(Collectors.toList());
        setResultTransformer(sqlQuery, aliasColumnName, tClass);
        sqlQuery.setResultTransformer(Transformers.aliasToBean(tClass));

        List<T> resultList = query.setMaxResults(pageable.getPageSize())
                                  .setFirstResult((int) pageable.getOffset())
                                  .getResultList();

        return new PageImpl<>(resultList, pageable, total);
    }

    public static <T> void setResultTransformer(SQLQuery sqlQuery, List<String> aliasColumnName, Class<T> tClass) {
        if (!CollectionUtils.isEmpty(aliasColumnName)) {
            Map<String, Field> mapFieldType = Arrays.stream(tClass.getDeclaredFields())
                                                    .collect(Collectors.toMap(Field::getName, field -> field));
            for (String alias : aliasColumnName) {
                Field field = mapFieldType.get(alias);
                if (field == null) {
                    continue;
                }
                sqlQuery.addScalar(alias, getHibernateType(field));
            }
        }
    }

    @SneakyThrows
    public static Type getHibernateType(Field field) {
        Class<?> fieldClass = field.getType();
        if (fieldClass.isAssignableFrom(Long.class)) {
            return LongType.INSTANCE;
        } else if (fieldClass.isAssignableFrom(Integer.class)) {
            return IntegerType.INSTANCE;
        } else if (fieldClass.isAssignableFrom(Double.class)) {
            return DoubleType.INSTANCE;
        } else if (fieldClass.isAssignableFrom(Float.class)) {
            return FloatType.INSTANCE;
        } else if (fieldClass.isAssignableFrom(String.class)) {
            return StringType.INSTANCE;
        } else if (fieldClass.isAssignableFrom(Date.class)) {
            return TimestampType.INSTANCE;
        } else if (fieldClass.isEnum()) {
            Properties parameters = new Properties();
            parameters.put(EnumType.ENUM, fieldClass.getName());

            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            if (enumerated != null && enumerated.value() == javax.persistence.EnumType.STRING) {
                parameters.put(EnumType.NAMED, true);
            } else {
                parameters.put(EnumType.NAMED, false);
                parameters.put(EnumType.TYPE, String.valueOf(Types.SMALLINT));
            }

            EnumType enumType = new EnumType();
            enumType.setTypeConfiguration(new TypeConfiguration());
            enumType.setParameterValues(parameters);

            return new CustomType(enumType);
        }
        throw new Exception("Không hỗ trợ kiểu dữ liệu này: " + fieldClass.getName());
    }

    /**
     * DungHT
     * <p>
     * Thuc hien set parameter cho query
     *
     * @param query         query
     * @param mapParams     danh sach cac tham so binh thuong
     * @param mapParamsList danh sach cac tham so kieu List
     */
    public static Query setParameter(Query query, Map<String, Object> mapParams, Map<String, List<?>> mapParamsList) {
        setParameter(query.unwrap(SQLQuery.class), mapParams, mapParamsList);
        return query;
    }

    /**
     * DungHT
     * <p>
     * Thuc hien set parameter cho query
     *
     * @param sqlQuery      sqlQuery
     * @param mapParams     danh sach cac tham so binh thuong
     * @param mapParamsList danh sach cac tham so kieu List
     */
    public static SQLQuery setParameter(SQLQuery sqlQuery, Map<String, Object> mapParams, Map<String, List<?>> mapParamsList) {
        if (mapParams != null && !mapParams.isEmpty()) {
            sqlQuery.setProperties(mapParams);
        }

        if (mapParamsList != null && !mapParamsList.isEmpty()) {
            for (Map.Entry<String, List<?>> entry : mapParamsList.entrySet()) {
                sqlQuery.setParameterList(entry.getKey(), entry.getValue());
            }
        }

        return sqlQuery;
    }

}
