--
-- Структура таблицы `children`
--

CREATE TABLE IF NOT EXISTS `children` (
                                          `id` int auto_increment primary key,
                                          `age` int DEFAULT NULL,
                                          `first_name` varchar(255) DEFAULT NULL,
                                          `last_name` varchar(255) DEFAULT NULL,
                                          `middle_name` varchar(255) DEFAULT NULL,
                                          `diagnoses_id` int DEFAULT NULL,
                                          `lessons_id` int DEFAULT NULL,
                                          `parents_id` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `diagnoses`
--

CREATE TABLE IF NOT EXISTS `diagnoses` (
                                           `id` int auto_increment primary key,
                                           `name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `lessons`
--

CREATE TABLE IF NOT EXISTS `lessons` (
                                         `id` int auto_increment primary key,
                                         `create_date` datetime(6) DEFAULT NULL,
                                         `group_type` enum('GROUP','INDIVIDUAL') DEFAULT NULL,
                                         `lesson_status` enum('CANCELLED','COMPLETED','RESERVED','SCHEDULED') DEFAULT NULL,
                                         `lesson_type` enum('AUTISM','DIKTOLOG','LOGOPED','PERMANENT','TRIAL') DEFAULT NULL,
                                         `update_date` datetime(6) DEFAULT NULL,
                                         `rooms_id` int DEFAULT NULL,
                                         `subjects_id` int DEFAULT NULL,
                                         `users_id` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `parents`
--

CREATE TABLE IF NOT EXISTS `parents` (
                                         `id` int auto_increment primary key,
                                         `first_name` varchar(255) DEFAULT NULL,
                                         `last_name` varchar(255) DEFAULT NULL,
                                         `mail` varchar(255) DEFAULT NULL,
                                         `middle_name` varchar(255) DEFAULT NULL,
                                         `phone_number` varchar(255) DEFAULT NULL,
                                         `registered_at` TIMESTAMP DEFAULT NULL,
                                         `chat_id` BIGINT DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `rooms`
--

CREATE TABLE IF NOT EXISTS `rooms` (
                                       `id` int auto_increment primary key,
                                       `name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `subjects`
--

CREATE TABLE IF NOT EXISTS `subjects` (
                                          `id` int auto_increment primary key,
                                          `name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `information`
--
CREATE TABLE IF NOT EXISTS `information` (
                                             `id` int auto_increment primary key,
                                             `text` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `teacher_subjects`
--

CREATE TABLE IF NOT EXISTS `teacher_subjects` (
                                                  `users_id` int auto_increment primary key,
                                                  `subjects_id` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `users`
--

CREATE TABLE IF NOT EXISTS `users` (
                                       `id` int auto_increment primary key,
                                       `authorities` enum('ADMIN','TEACHER') DEFAULT NULL,
                                       `email` varchar(255) DEFAULT NULL,
                                       `login` varchar(255) DEFAULT NULL,
                                       `name` varchar(255) DEFAULT NULL,
                                       `password` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
