package kidd.house.zerde.dto.lockLesson;

public record LockLessonRequest(
        String lockDateTimeFrom,
        String lockDateTimeTo,
        int roomId
) {
}
