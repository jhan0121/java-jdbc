package com.interface21.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.interface21.dao.DataAccessException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JdbcTemplateTest {

    private DataSource dataSource = mock();

    private Connection connection = mock();

    private PreparedStatement preparedStatement = mock();

    private ResultSet resultSet = mock();

    private JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSource);

    @BeforeEach
    void setUp() throws Exception {
        dataSource = mock(DataSource.class);
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        resultSet = mock(ResultSet.class);

        given(dataSource.getConnection()).willReturn(connection);
        given(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).willReturn(preparedStatement);

        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    @DisplayName("queryForObject 단건 조회 성공")
    void queryForObject_success() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE id = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(true);
        given(resultSet.getString("name")).willReturn("test");

        // when
        final String name = jdbcTemplate.queryForObject(sql, rowMapper, 1L);

        // then
        assertThat(name).isEqualTo("test");

        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).executeQuery();
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("queryForObject 결과가 없으면 null 반환")
    void queryForObject_noResult() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE id = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(false);

        // when
        final String name = jdbcTemplate.queryForObject(sql, rowMapper, 1L);

        // then
        assertThat(name).isNull();

        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).executeQuery();
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("queryForObject SQLException 발생 시 DataAccessException으로 변환")
    void queryForObject_sqlException() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE id = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willThrow(new SQLException("SQL Error"));

        // when & then
        assertThatThrownBy(() -> jdbcTemplate.queryForObject(sql, rowMapper, 1L))
                .isInstanceOf(DataAccessException.class)
                .hasCauseInstanceOf(SQLException.class);

        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("query 여러 행 조회 성공")
    void query_success() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE is_alive = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willReturn(resultSet);
        given(resultSet.next())
                .willReturn(true)
                .willReturn(true)
                .willReturn(false);
        given(resultSet.getString("name"))
                .willReturn("test1")
                .willReturn("test2");

        // when
        final List<String> names = jdbcTemplate.query(sql, rowMapper, true);

        // then
        assertThat(names).containsExactlyInAnyOrderElementsOf(List.of("test1", "test2"));

        verify(preparedStatement).setBoolean(1, true);
        verify(preparedStatement).executeQuery();
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("query 결과가 없으면 빈 리스트 반환")
    void query_emptyResult() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE is_alive = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willReturn(resultSet);
        given(resultSet.next()).willReturn(false);

        // when
        final List<String> names = jdbcTemplate.query(sql, rowMapper, true);

        // then
        assertThat(names).isEmpty();

        verify(preparedStatement).setBoolean(1, true);
        verify(preparedStatement).executeQuery();
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("query SQLException 발생 시 DataAccessException으로 변환")
    void query_sqlException() throws Exception {
        // given
        final String sql = "SELECT name FROM users WHERE id = ?";
        final RowMapper<String> rowMapper = rs -> rs.getString("name");

        given(preparedStatement.executeQuery()).willThrow(new SQLException("SQL Error"));

        // when & then
        assertThatThrownBy(() -> jdbcTemplate.query(sql, rowMapper, 1L))
                .isInstanceOf(DataAccessException.class)
                .hasCauseInstanceOf(SQLException.class);

        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("단일 파라미터로 update 성공")
    void update_success() throws Exception {
        // given
        final String sql = "INSERT INTO users (name) VALUES (?)";

        given(preparedStatement.executeUpdate()).willReturn(1);

        // when
        jdbcTemplate.update(sql, "test1");

        // then
        verify(preparedStatement).setString(1, "test1");
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("여러 파라미터로 update 성공")
    void update_withMultipleParameters() throws Exception {
        // given
        final String sql = "INSERT INTO users (name, is_alive) VALUES (?, ?)";

        given(preparedStatement.executeUpdate()).willReturn(1);

        // when
        jdbcTemplate.update(sql, "test1", true);

        // then
        verify(preparedStatement).setString(1, "test1");
        verify(preparedStatement).setBoolean(2, true);
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("update SQLException 발생 시 DataAccessException으로 변환")
    void update_sqlException() throws Exception {
        // given
        final String sql = "INSERT INTO users (name, is_alive) VALUES (?)";

        given(preparedStatement.executeUpdate()).willThrow(new SQLException("SQL Error"));

        // when & then
        assertThatThrownBy(() -> jdbcTemplate.update(sql, "test"))
                .isInstanceOf(DataAccessException.class)
                .hasCauseInstanceOf(SQLException.class);

        verify(preparedStatement).setString(1, "test");
        verify(preparedStatement).close();
        verify(connection).close();
    }
}
