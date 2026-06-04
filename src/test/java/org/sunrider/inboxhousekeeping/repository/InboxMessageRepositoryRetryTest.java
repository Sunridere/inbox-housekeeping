package org.sunrider.inboxhousekeeping.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest(properties = "spring.liquibase.enabled=false")
@ActiveProfiles("test")
public class InboxMessageRepositoryRetryTest {

    @Autowired
    private InboxMessageRepository repository;

    @MockitoBean
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Временную ошибку (CannotAcquireLockException) ретраит и со 2-й попытки возвращает результат")
    void retriesTransientErrorAndSucceeds () {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), anyInt()))
            .thenThrow(new CannotAcquireLockException("boom"))
            .thenReturn(List.of("inbox_message_2026_01_01"));

        List<String> result = repository.getOldPartitions(1);
        assertThat(result).containsExactly("inbox_message_2026_01_01");
        verify(jdbcTemplate, times(2)).queryForList(anyString(), eq(String.class), anyInt());
    }

    @Test
    @DisplayName("Игнорируемую ошибку (DataIntegrityViolationException) не ретраит — пробрасывает с 1-й попытки")
    void doesNotRetryIgnoredExceptionAndRethrows () {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), anyInt()))
            .thenThrow(new DataIntegrityViolationException("boom"));

        assertThrows(DataIntegrityViolationException.class, () -> repository.getOldPartitions(1));
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(String.class), anyInt());
    }

    @Test
    @DisplayName("При постоянной временной ошибке делает ровно 3 попытки и пробрасывает исключение")
    void propagatesErrorAfterMaxAttemptsExhausted(){
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), anyInt()))
            .thenThrow(new CannotAcquireLockException("boom"));
        assertThrows(CannotAcquireLockException.class, () -> repository.getOldPartitions(1));
        verify(jdbcTemplate, times(3)).queryForList(anyString(), eq(String.class), anyInt());
    }

}
