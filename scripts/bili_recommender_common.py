#!/usr/bin/env python3
"""
B站推荐系统公共工具模块

职责：
1. 统一读取环境变量与默认配置
2. 提供 PostgreSQL 连接与事务辅助
3. 提供常见数据解析工具（标签、关键词、向量）
4. 提供基础日志输出

说明：
- 本模块只提供可复用能力，不直接执行业务流程。
- 业务脚本统一 import 本模块，以保持代码风格一致并减少重复逻辑。
"""

from __future__ import annotations

import json
import os
import re
import sys
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection


@dataclass
class AppConfig:
    """运行时配置。默认值来自项目需求，可被环境变量覆盖。"""

    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_user: str = os.getenv("DB_USER", "root")
    db_password: str = os.getenv("DB_PASSWORD", "15671040800q")
    db_name: str = os.getenv("DB_NAME", "media_knowledge_base")

    bili_uid: int = int(os.getenv("BILI_UID", "1512253857"))
    bili_cookie: str = os.getenv("BILIBILI_COOKIE", "")


CONFIG = AppConfig()


def log(message: str, level: str = "INFO") -> None:
    """统一日志格式，方便脚本串联时定位问题。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [{level}] {message}")


def log_exception(prefix: str, exc: Exception) -> None:
    """打印异常与堆栈，用于快速定位问题。"""
    log(f"{prefix}: {exc}", level="ERROR")
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    print(tb, file=sys.stderr)


@contextmanager
def get_db_conn(autocommit: bool = False) -> Generator[PgConnection, None, None]:
    """
    获取数据库连接，并自动管理提交/回滚。

    参数：
    - autocommit: 是否开启自动提交，默认 False。
    """
    conn: Optional[PgConnection] = None
    try:
        conn = psycopg2.connect(
            host=CONFIG.db_host,
            port=CONFIG.db_port,
            user=CONFIG.db_user,
            password=CONFIG.db_password,
            dbname=CONFIG.db_name,
        )
        conn.autocommit = autocommit
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if conn is not None and not autocommit:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()


def fetch_all_dict(conn: PgConnection, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    """执行查询并返回字典列表。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def fetch_one_dict(conn: PgConnection, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    """执行查询并返回单行字典。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def split_keywords(text: str) -> List[str]:
    """
    将文本切分为简单关键词。

    说明：
    - 为兼容中英文，这里采用保守规则：先按非文字字符切分，再过滤长度过短项。
    - 该方法不依赖第三方分词包，便于脚本环境直接运行。
    """
    if not text:
        return []
    parts = re.split(r"[^\w\u4e00-\u9fff]+", text.lower())
    return [p for p in parts if len(p) >= 2]


def normalize_tags(raw: Any) -> List[str]:
    """
    统一解析 tags 字段，兼容以下存储形态：
    - PostgreSQL text[]
    - JSON 字符串
    - 逗号分隔字符串
    """
    if raw is None:
        return []

    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]

    if isinstance(raw, tuple):
        return [str(x).strip() for x in raw if str(x).strip()]

    raw_text = str(raw).strip()
    if not raw_text:
        return []

    # 优先尝试 JSON
    if raw_text.startswith("[") and raw_text.endswith("]"):
        try:
            value = json.loads(raw_text)
            if isinstance(value, list):
                return [str(x).strip() for x in value if str(x).strip()]
        except json.JSONDecodeError:
            pass

    # 兜底：逗号分隔
    return [x.strip() for x in raw_text.split(",") if x.strip()]


def json_dumps(data: Any) -> str:
    """统一 JSON 输出，确保中文不转义。"""
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def table_columns(conn: PgConnection, table_name: str) -> List[str]:
    """读取表字段名，便于脚本动态兼容已有表结构。"""
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = %s
    ORDER BY ordinal_position
    """
    rows = fetch_all_dict(conn, sql, (table_name,))
    return [r["column_name"] for r in rows]


def parse_vector_text(vector_text: Any) -> Optional[List[float]]:
    """将 pgvector 文本形式（如 [0.1,0.2]）转为 float 列表。"""
    if vector_text is None:
        return None
    txt = str(vector_text).strip()
    if not txt:
        return None
    txt = txt.strip("[]")
    if not txt:
        return None
    try:
        return [float(x) for x in txt.split(",")]
    except ValueError:
        return None


def cosine_similarity(v1: Sequence[float], v2: Sequence[float]) -> float:
    """计算余弦相似度，异常或零向量返回 0。"""
    if not v1 or not v2:
        return 0.0
    if len(v1) != len(v2):
        return 0.0

    dot = 0.0
    n1 = 0.0
    n2 = 0.0
    for a, b in zip(v1, v2):
        dot += a * b
        n1 += a * a
        n2 += b * b

    if n1 <= 0 or n2 <= 0:
        return 0.0
    return dot / ((n1 ** 0.5) * (n2 ** 0.5))


def average_vectors(vectors: Iterable[Sequence[float]]) -> Optional[List[float]]:
    """对多个向量求均值；若输入为空或维度不一致返回 None。"""
    vectors = list(vectors)
    if not vectors:
        return None

    dim = len(vectors[0])
    if dim == 0:
        return None
    if any(len(v) != dim for v in vectors):
        return None

    sums = [0.0] * dim
    for vec in vectors:
        for i, value in enumerate(vec):
            sums[i] += float(value)

    count = float(len(vectors))
    return [x / count for x in sums]


def to_pgvector_literal(vec: Sequence[float]) -> str:
    """将向量转为 pgvector SQL 字面量。"""
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"
