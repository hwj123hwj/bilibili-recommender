#!/usr/bin/env python3
"""
初始化 B站推荐系统数据库结构。

创建内容：
1. pgvector 扩展
2. 用户表 recommender_users
3. 关注表 user_followings
4. 新视频缓存表 bili_new_videos
5. 兴趣标签表 user_interest_tags
6. 推荐日志表 recommendation_logs

使用示例：
- python3 scripts/init_recommendation_db.py
- python3 scripts/init_recommendation_db.py --drop-and-recreate
"""

from __future__ import annotations

import argparse
import sys

from bili_recommender_common import CONFIG, get_db_conn, log, log_exception


DDL_STATEMENTS = [
    # pgvector 是后续向量相似度计算的基础能力
    "CREATE EXTENSION IF NOT EXISTS vector",
    """
    CREATE TABLE IF NOT EXISTS recommender_users (
        id SERIAL PRIMARY KEY,
        uid BIGINT NOT NULL UNIQUE,
        nickname VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_followings (
        id SERIAL PRIMARY KEY,
        uid BIGINT NOT NULL,
        up_mid BIGINT NOT NULL,
        up_name VARCHAR(255),
        followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(uid, up_mid)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bili_new_videos (
        id SERIAL PRIMARY KEY,
        bvid VARCHAR(50) NOT NULL UNIQUE,
        title TEXT,
        up_mid BIGINT,
        up_name VARCHAR(255),
        pubdate TIMESTAMP,
        duration INTEGER,
        tags TEXT[],
        description TEXT,
        content_text TEXT,
        content_vector vector(1024),
        discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_interest_tags (
        id SERIAL PRIMARY KEY,
        uid BIGINT NOT NULL,
        tag_name VARCHAR(100) NOT NULL,
        source VARCHAR(30) DEFAULT 'tag',
        weight FLOAT DEFAULT 1.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(uid, tag_name, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recommendation_logs (
        id SERIAL PRIMARY KEY,
        uid BIGINT NOT NULL,
        bvid VARCHAR(50) NOT NULL,
        recommended_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        score FLOAT DEFAULT 0.0,
        score_tags FLOAT DEFAULT 0.0,
        score_keywords FLOAT DEFAULT 0.0,
        score_vector FLOAT DEFAULT 0.0,
        score_up FLOAT DEFAULT 0.0,
        reason TEXT,
        status VARCHAR(20) DEFAULT 'pending',
        UNIQUE(uid, bvid)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_followings_uid ON user_followings(uid)",
    "CREATE INDEX IF NOT EXISTS idx_user_followings_up_mid ON user_followings(up_mid)",
    "CREATE INDEX IF NOT EXISTS idx_bili_new_videos_up_mid ON bili_new_videos(up_mid)",
    "CREATE INDEX IF NOT EXISTS idx_bili_new_videos_pubdate ON bili_new_videos(pubdate DESC)",
    "CREATE INDEX IF NOT EXISTS idx_user_interest_tags_uid_weight ON user_interest_tags(uid, weight DESC)",
    "CREATE INDEX IF NOT EXISTS idx_recommendation_logs_uid_status ON recommendation_logs(uid, status)",
    "CREATE INDEX IF NOT EXISTS idx_recommendation_logs_uid_score ON recommendation_logs(uid, score DESC)",
]

DROP_STATEMENTS = [
    "DROP TABLE IF EXISTS recommendation_logs",
    "DROP TABLE IF EXISTS user_interest_tags",
    "DROP TABLE IF EXISTS bili_new_videos",
    "DROP TABLE IF EXISTS user_followings",
    "DROP TABLE IF EXISTS recommender_users",
]


def init_db(drop_and_recreate: bool = False) -> None:
    """执行数据库初始化流程。"""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            if drop_and_recreate:
                log("检测到 --drop-and-recreate，开始清理旧表")
                for sql in DROP_STATEMENTS:
                    cur.execute(sql)

            for sql in DDL_STATEMENTS:
                cur.execute(sql)

            # 确保默认用户记录存在，方便后续模块直接使用 uid
            cur.execute(
                """
                INSERT INTO recommender_users(uid, nickname)
                VALUES (%s, %s)
                ON CONFLICT (uid)
                DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                """,
                (CONFIG.bili_uid, f"uid_{CONFIG.bili_uid}"),
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="初始化 B站推荐系统数据库")
    parser.add_argument(
        "--drop-and-recreate",
        action="store_true",
        help="先删除推荐系统相关表再重建（谨慎使用）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        log("开始初始化数据库结构")
        init_db(drop_and_recreate=args.drop_and_recreate)
        log("数据库初始化完成")
        return 0
    except Exception as exc:
        log_exception("数据库初始化失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
