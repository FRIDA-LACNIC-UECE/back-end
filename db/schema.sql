CREATE DATABASE IF NOT EXISTS frida;
USE frida;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for alembic_version
-- ----------------------------
DROP TABLE IF EXISTS `alembic_version`;
CREATE TABLE `alembic_version`  (
  `version_num` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`version_num`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of alembic_version
-- ----------------------------
INSERT INTO `alembic_version` VALUES ('a6fb53c24de2');

-- ----------------------------
-- Table structure for anonymization_types
-- ----------------------------
DROP TABLE IF EXISTS `anonymization_types`;
CREATE TABLE `anonymization_types`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of anonymization_types
-- ----------------------------

-- ----------------------------
-- Table structure for anonymizations
-- ----------------------------
DROP TABLE IF EXISTS `anonymizations`;
CREATE TABLE `anonymizations`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_database` int NOT NULL,
  `id_anonymization_type` int NOT NULL,
  `table` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `columns` json NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `id_database`(`id_database` ASC) USING BTREE,
  INDEX `id_anonymization_type`(`id_anonymization_type` ASC) USING BTREE,
  CONSTRAINT `anonymizations_ibfk_1` FOREIGN KEY (`id_database`) REFERENCES `databases` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `anonymizations_ibfk_2` FOREIGN KEY (`id_anonymization_type`) REFERENCES `anonymization_types` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of anonymizations
-- ----------------------------

-- ----------------------------
-- Table structure for databases
-- ----------------------------
DROP TABLE IF EXISTS `databases`;
CREATE TABLE `databases`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_db` int NOT NULL,
  `id_db_type` int NOT NULL,
  `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `host` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `user` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `port` int NOT NULL,
  `password` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `ssh` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `id_db`(`id_db` ASC) USING BTREE,
  INDEX `id_db_type`(`id_db_type` ASC) USING BTREE,
  CONSTRAINT `databases_ibfk_1` FOREIGN KEY (`id_db`) REFERENCES `users` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `databases_ibfk_2` FOREIGN KEY (`id_db_type`) REFERENCES `valid_databases` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 6 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of databases
-- ----------------------------

-- ----------------------------
-- Table structure for databases_keys
-- ----------------------------
CREATE TABLE `databases_keys` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `id_db` int(11) NOT NULL,
  `public_key` text NOT NULL,
  `private_key` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `id_db` (`id_db`),
  CONSTRAINT `databases_keys_ibfk_1` FOREIGN KEY (`id_db`) REFERENCES `databases` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

-- ----------------------------
-- Records of databases_keys
-- ----------------------------

-- ----------------------------
-- Table structure for users
-- ----------------------------
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `email` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `password` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `is_admin` int NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of users
-- ----------------------------

-- ----------------------------
-- Table structure for valid_databases
-- ----------------------------
DROP TABLE IF EXISTS `valid_databases`;
CREATE TABLE `valid_databases`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of valid_databases
-- ----------------------------
INSERT INTO `valid_databases` VALUES (1, 'mysql');

SET FOREIGN_KEY_CHECKS = 1;
