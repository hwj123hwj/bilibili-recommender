#!/usr/bin/env python3
"""
构建用户兴趣画像：统计历史视频中的标签与关键词，并写入 user_interest_tags。
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from bili_recommender_common import (
    CONFIG,
    fetch_all_dict,
    get_db_conn,
    json_dumps,
    log,
    log_exception,
    normalize_tags,
    split_keywords,
    table_columns,
)


STOPWORDS = {
    "的",
    "了",
    "和",
    "是",
    "在",
    "就",
    "都",
    "也",
    "与",
    "及",
    "并",
    "一个",
    "这个",
    "那个",
    "我们",
    "你们",
    "他们",
    "视频",
    "教程",
    "分享",
    "官方",
    "最新",
    "全集",
    "合集",
    "the",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "you",
    "your",
    "are",
    "was",
    "have",
    "has",
    "new",
    "video",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="用户兴趣画像建模")
    parser.add_argument("--uid", type=int, default=CONFIG.bili_uid, help="用户 UID")
    parser.add_argument("--months", type=int, default=6, help="回看最近 N 个月历史")
    parser.add_argument("--limit", type=int, default=1500, help="最多分析视频数量")
    parser.add_argument("--top-tags", type=int, default=60, help="保留标签 TopN")
    parser.add_argument("--top-keywords", type=int, default=120, help="保留关键词 TopN")
    parser.add_argument("--min-tag-weight", type=float, default=0.3, help="标签最小权重")
    parser.add_argument("--min-keyword-freq", type=float, default=2.0, help="关键词最小频次")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    parser.add_argument("--dry-run", action="store_true", help="只分析不写库")
    return parser.parse_args()


def pick_time_column(columns: Sequence[str]) -> Optional[str]:
    candidates = ["viewed_at", "watched_at", "favorite_at", "pubdate", "created_at", "updated_at"]
    for col in candidates:
        if col in columns:
            return col
    return None


def load_history_videos(months: int, limit: int) -> List[Dict[str, Any]]:
    with get_db_conn() as conn:
        cols = set(table_columns(conn, "bili_video_contents"))

        select_parts = ["bvid"]
        for col in ["title", "description", "content_text", "tags", "content_vector"]:
            if col in cols:
                select_parts.append(col)
            else:
                select_parts.append(f"NULL AS {col}")

        time_col = pick_time_column(cols)
        if time_col:
            select_parts.append(f"{time_col} AS event_time")
        else:
            select_parts.append("NULL AS event_time")

        sql = f"""
        SELECT {', '.join(select_parts)}
        FROM bili_video_contents
        ORDER BY {time_col if time_col else 'bvid'} DESC
        LIMIT %s
        """
        rows = fetch_all_dict(conn, sql, (limit,))

    cutoff = datetime.now() - timedelta(days=max(months, 1) * 30)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        event_time = row.get("event_time")
        if isinstance(event_time, datetime) and event_time < cutoff:
            continue
        filtered.append(row)
    return filtered


def age_decay(event_time: Any) -> float:
    if not isinstance(event_time, datetime):
        return 0.85
    days = max((datetime.now() - event_time).days, 0)
    return 1.0 / (1.0 + days / 45.0)


def extract_keywords(*texts: Any) -> List[str]:
    tokens: List[str] = []
    for text in texts:
        for token in split_keywords(str(text or "")):
            if token in STOPWORDS:
                continue
            if token.isdigit():
                continue
            tokens.append(token)
    return tokens


def build_interest_profile(
    videos: List[Dict[str, Any]],
    top_tags: int,
    top_keywords: int,
    min_tag_weight: float,
    min_keyword_freq: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    tag_counter: Counter[str] = Counter()
    keyword_counter: Counter[str] = Counter()

    for v in videos:
        decay = age_decay(v.get("event_time"))
        tags = normalize_tags(v.get("tags"))
        for tag in tags:
            cleaned = str(tag).strip().lower()
            if len(cleaned) < 2:
                continue
            tag_counter[cleaned] += decay

        words = extract_keywords(v.get("title"), v.get("description"), v.get("content_text"), " ".join(tags))
        for w in words:
            keyword_counter[w] += decay

    tags_ranked = [
        {"tag_name": k, "source": "tag", "weight": round(float(v), 4)}
        for k, v in tag_counter.most_common(top_tags)
        if v >= min_tag_weight
    ]
    keywords_ranked = [
        {"tag_name": k, "source": "keyword", "weight": round(float(v), 4)}
        for k, v in keyword_counter.most_common(top_keywords)
        if v >= min_keyword_freq
    ]

    return tags_ranked, keywords_ranked


def save_interest_tags(uid: int, interests: List[Dict[str, Any]]) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_interest_tags WHERE uid = %s AND source = ANY(%s)",
                (uid, ["tag", "keyword"]),
            )
            for item in interests:
                cur.execute(
                    """
                    INSERT INTO user_interest_tags(uid, tag_name, source, weight)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (uid, tag_name, source)
                    DO UPDATE SET weight = EXCLUDED.weight, updated_at = CURRENT_TIMESTAMP
                    """,
                    (uid, item["tag_name"], item["source"], item["weight"]),
                )


def run(
    uid: int,
    months: int,
    limit: int,
    top_tags: int,
    top_keywords: int,
    min_tag_weight: float,
    min_keyword_freq: float,
    dry_run: bool,
) -> Dict[str, Any]:
    videos = load_history_videos(months=months, limit=limit)
    tags_ranked, keywords_ranked = build_interest_profile(
        videos=videos,
        top_tags=top_tags,
        top_keywords=top_keywords,
        min_tag_weight=min_tag_weight,
        min_keyword_freq=min_keyword_freq,
    )
    merged = tags_ranked + keywords_ranked

    if not dry_run:
        save_interest_tags(uid=uid, interests=merged)

    return {
        "uid": uid,
        "analyzed_videos": len(videos),
        "tag_count": len(tags_ranked),
        "keyword_count": len(keywords_ranked),
        "interests": merged,
    }


def main() -> int:
    args = parse_args()
    try:
        result = run(
            uid=args.uid,
            months=args.months,
            limit=args.limit,
            top_tags=args.top_tags,
            top_keywords=args.top_keywords,
            min_tag_weight=args.min_tag_weight,
            min_keyword_freq=args.min_keyword_freq,
            dry_run=args.dry_run,
        )
        if args.json:
            print(json_dumps(result))
        else:
            log(f"分析视频数: {result['analyzed_videos']}")
            log(f"兴趣标签数: {result['tag_count']}")
            log(f"兴趣关键词数: {result['keyword_count']}")
            preview = result["interests"][:15]
            if preview:
                log("Top 兴趣项: " + ", ".join(f"{x['tag_name']}({x['weight']:.2f})" for x in preview))
        return 0
    except Exception as exc:
        log_exception("兴趣画像构建失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
