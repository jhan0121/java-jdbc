package com.interface21.jdbc.core;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
interface JdbcCallback<T> {
    T execute(PreparedStatement preparedStatement) throws SQLException;
}
