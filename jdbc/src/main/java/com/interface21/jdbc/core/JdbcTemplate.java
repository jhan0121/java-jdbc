package com.interface21.jdbc.core;

import com.interface21.dao.DataAccessException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);

    private final DataSource dataSource;
    private final ParameterBinder parameterBinder;

    public JdbcTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
        this.parameterBinder = ParameterBinder.init();
    }

    public int update(
            final String sql,
            final Object... parameters
    ) {
        return executeSql(sql, PreparedStatement::executeUpdate, parameters);
    }

    public <T> List<T> query(
            final String sql,
            final RowMapper<T> rowMapper,
            final Object... parameters
    ) {
        return executeSql(sql, preparedStatement -> {
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<T> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(rowMapper.mapRow(resultSet));
                }
                return result;
            }
        }, parameters);
    }

    public <T> T queryForObject(
            final String sql,
            final RowMapper<T> rowMapper,
            final Object... parameters
    ) {
        return executeSql(sql, preparedStatement -> {
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return rowMapper.mapRow(resultSet);
                }
                return null;
            }
        }, parameters);
    }

    private <T> T executeSql(String sql, JdbcCallback<T> callback, Object... parameters) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.debug("query : {}", sql);
            bindParameters(preparedStatement, parameters);

            return callback.execute(preparedStatement);

        } catch (final SQLException e) {
            log.error(e.getMessage(), e);
            throw new DataAccessException(e);
        }
    }

    private void bindParameters(
            final PreparedStatement preparedStatement,
            final Object... parameters
    ) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            parameterBinder.bindParameter(preparedStatement, i + 1, parameters[i]);
        }
    }
}
