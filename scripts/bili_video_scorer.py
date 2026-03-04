#!/usr/bin/env python3
"""
对新视频进行兴趣打分，并写入 recommendation_logs。

评分维度（0-100）：
- Tags 40%
- Keywords 30%
- Vector 20%
- UP 10%
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from bili_recommender_common import (
    CONFIG,
    fetch_all_dict,
    get_db_conn,
    json_dumps,
    log,
    log_exception,
    normalize_tags,
    parse_vector_text,
    split_keywords,
    table_columns,
    to_pgvector_literal,
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
    "我们",
    "你们",
    "他们",
    "视频",
    "教程",
    "分享",
    "官方",
    "最新",
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
    parser = argparse.ArgumentParser(description="B站视频兴趣评分")
    parser.add_argument("--uid", type=int, default=CONFIG.bili_uid, help="用户 UID")
    parser.add_argument("--limit", type=int, default=200, help="最多处理视频数")
    parser.add_argument("--min-score", type=float, default=0.0, help="最低综合分阈值（0-100）")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    parser.add_argument("--dry-run", action="store_true", help="只计算，不写 recommendation_logs")
    return parser.parse_args()


def load_user_interests(uid: int) -> Tuple[Dict[str, float], Dict[str, float]]:
    with get_db_conn() as conn:
        rows = fetch_all_dict(
            conn,
            """
            SELECT tag_name, source, weight
            FROM user_interest_tags
            WHERE uid = %s
            ORDER BY weight DESC
            """,
            (uid,),
        )

    tag_weights: Dict[str, float] = {}
    keyword_weights: Dict[str, float] = {}
    for row in rows:
        name = str(row.get("tag_name") or "").strip().lower()
        if not name:
            continue
        weight = float(row.get("weight") or 0.0)
        source = str(row.get("source") or "").strip().lower()
        if source == "keyword":
            keyword_weights[name] = max(keyword_weights.get(name, 0.0), weight)
        else:
            tag_weights[name] = max(tag_weights.get(name, 0.0), weight)
    return tag_weights, keyword_weights


def load_following_ups(uid: int) -> Set[int]:
    with get_db_conn() as conn:
        rows = fetch_all_dict(conn, "SELECT up_mid FROM user_followings WHERE uid = %s", (uid,))
    return {int(r["up_mid"]) for r in rows if r.get("up_mid") is not None}


def build_user_interest_vector(limit: int = 500) -> Optional[List[float]]:
    with get_db_conn() as conn:
        cols = set(table_columns(conn, "bili_video_contents"))
        if "content_vector" not in cols:
            return None

        order_col = "updated_at" if "updated_at" in cols else ("created_at" if "created_at" in cols else "bvid")
        rows = fetch_all_dict(
            conn,
            f"""
            SELECT AVG(content_vector)::text AS vec
            FROM (
                SELECT content_vector
                FROM bili_video_contents
                WHERE content_vector IS NOT NULL
                ORDER BY {order_col} DESC
                LIMIT %s
            ) t
            """,
            (limit,),
        )

    if not rows:
        return None
    return parse_vector_text(rows[0].get("vec"))


def tokenize_video_keywords(title: str, description: str, tags: Sequence[str]) -> Set[str]:
    text = " ".join([title or "", description or "", " ".join(tags or [])])
    return {
        t
        for t in split_keywords(text)
        if t not in STOPWORDS and not t.isdigit() and len(t) >= 2
    }


def calc_tag_score(video_tags: Sequence[str], user_tag_weights: Dict[str, float]) -> Tuple[float, List[str]]:
    tags = [str(t).strip().lower() for t in video_tags if str(t).strip()]
    if not tags or not user_tag_weights:
        return 0.0, []

    max_w = max(user_tag_weights.values()) if user_tag_weights else 0.0
    if max_w <= 0:
        return 0.0, []

    matched: List[str] = []
    total = 0.0
    for tag in tags:
        if tag in user_tag_weights:
            matched.append(tag)
            total += user_tag_weights[tag] / max_w

    score = min(1.0, total / max(len(set(tags)), 1))
    return score * 100.0, matched[:8]


def calc_keyword_score(tokens: Set[str], user_keyword_weights: Dict[str, float]) -> Tuple[float, List[str]]:
    if not tokens or not user_keyword_weights:
        return 0.0, []

    max_w = max(user_keyword_weights.values()) if user_keyword_weights else 0.0
    if max_w <= 0:
        return 0.0, []

    matched = [t for t in tokens if t in user_keyword_weights]
    total = sum(user_keyword_weights[t] / max_w for t in matched)
    denom = max(min(len(tokens), 24), 1)
    score = min(1.0, total / denom)
    return score * 100.0, sorted(matched)[:10]


def build_candidates(uid: int, limit: int, user_vector: Optional[Sequence[float]]) -> List[Dict[str, Any]]:
    with get_db_conn() as conn:
        cols = set(table_columns(conn, "bili_new_videos"))
        if "bvid" not in cols:
            return []

        select_parts = [
            "bvid",
            "COALESCE(title, '') AS title" if "title" in cols else "'' AS title",
            "COALESCE(up_mid, 0) AS up_mid" if "up_mid" in cols else "0 AS up_mid",
            "COALESCE(up_name, '') AS up_name" if "up_name" in cols else "'' AS up_name",
            "tags" if "tags" in cols else "NULL AS tags",
            "COALESCE(description, '') AS description" if "description" in cols else "'' AS description",
        ]

        params: List[Any] = [uid]
        vector_sql = "0.0 AS vector_similarity"
        if user_vector and "content_vector" in cols:
            vector_sql = "CASE WHEN content_vector IS NULL THEN 0.0 ELSE GREATEST(0.0, LEAST(1.0, 1 - (content_vector <=> %s::vector))) END AS vector_similarity"
            params.append(to_pgvector_literal(user_vector))
        select_parts.append(vector_sql)

        order_col = "pubdate" if "pubdate" in cols else "updated_at"
        params.append(limit)

        sql = f"""
        SELECT {', '.join(select_parts)}
        FROM bili_new_videos nv
        WHERE NOT EXISTS (
            SELECT 1
            FROM recommendation_logs rl
            WHERE rl.uid = %s AND rl.bvid = nv.bvid AND rl.status IN ('liked', 'disliked')
        )
        ORDER BY {order_col} DESC NULLS LAST
        LIMIT %s
        """
        return fetch_all_dict(conn, sql, params)


def build_reason(matched_tags: List[str], matched_keywords: List[str], vector_score: float, up_score: float) -> str:
    parts: List[str] = []
    if matched_tags:
        parts.append("标签匹配: " + ", ".join(matched_tags[:4]))
    if matched_keywords:
        parts.append("关键词匹配: " + ", ".join(matched_keywords[:4]))
    if vector_score >= 60:
        parts.append("向量相似度高")
    elif vector_score >= 30:
        parts.append("向量相似度中等")
    if up_score >= 100:
        parts.append("来自已关注 UP")
    return "；".join(parts) if parts else "综合匹配度一般"


def save_scores(uid: int, scored: List[Dict[str, Any]]) -> None:
    if not scored:
        return

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for item in scored:
                cur.execute(
                    """
                    INSERT INTO recommendation_logs(
                        uid, bvid, score, score_tags, score_keywords, score_vector, score_up, reason, status, recommended_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', CURRENT_TIMESTAMP)
                    ON CONFLICT (uid, bvid)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        score_tags = EXCLUDED.score_tags,
                        score_keywords = EXCLUDED.score_keywords,
                        score_vector = EXCLUDED.score_vector,
                        score_up = EXCLUDED.score_up,
                        reason = EXCLUDED.reason,
                        recommended_at = CURRENT_TIMESTAMP,
                        status = CASE
                            WHEN recommendation_logs.status IN ('liked', 'disliked') THEN recommendation_logs.status
                            ELSE 'pending'
                        END
                    """,
                    (
                        uid,
                        item["bvid"],
                        item["score"],
                        item["score_tags"],
                        item["score_keywords"],
                        item["score_vector"],
                        item["score_up"],
                        item["reason"],
                    ),
                )


def run(uid: int, limit: int, min_score: float, dry_run: bool = False) -> Dict[str, Any]:
    user_tag_weights, user_keyword_weights = load_user_interests(uid)
    following_ups = load_following_ups(uid)
    user_vector = build_user_interest_vector(limit=500)

    candidates = build_candidates(uid=uid, limit=limit, user_vector=user_vector)
    scored: List[Dict[str, Any]] = []

    for item in candidates:
        video_tags = normalize_tags(item.get("tags"))
        tag_score, matched_tags = calc_tag_score(video_tags, user_tag_weights)

        tokens = tokenize_video_keywords(item.get("title") or "", item.get("description") or "", video_tags)
        keyword_score, matched_keywords = calc_keyword_score(tokens, user_keyword_weights)

        vector_score = max(0.0, min(100.0, float(item.get("vector_similarity") or 0.0) * 100.0))
        up_score = 100.0 if int(item.get("up_mid") or 0) in following_ups else 0.0

        total_score = (tag_score * 0.4) + (keyword_score * 0.3) + (vector_score * 0.2) + (up_score * 0.1)
        if total_score < min_score:
            continue

        reason = build_reason(
            matched_tags=matched_tags,
            matched_keywords=matched_keywords,
            vector_score=vector_score,
            up_score=up_score,
        )

        scored.append(
            {
                "bvid": item["bvid"],
                "title": item.get("title") or "",
                "up_mid": int(item.get("up_mid") or 0),
                "up_name": item.get("up_name") or "",
                "score": round(total_score, 2),
                "score_tags": round(tag_score, 2),
                "score_keywords": round(keyword_score, 2),
                "score_vector": round(vector_score, 2),
                "score_up": round(up_score, 2),
                "reason": reason,
            }
        )

    scored.sort(key=lambda x: x["score"], reverse=True)

    if not dry_run:
        save_scores(uid=uid, scored=scored)

    return {
        "uid": uid,
        "candidate_count": len(candidates),
        "scored_count": len(scored),
        "saved_count": 0 if dry_run else len(scored),
        "scores": scored,
    }


def main() -> int:
    args = parse_args()
    try:
        result = run(uid=args.uid, limit=args.limit, min_score=args.min_score, dry_run=args.dry_run)
        if args.json:
            print(json_dumps(result))
        else:
            log(f"候选视频数: {result['candidate_count']}")
            log(f"达标并打分数: {result['scored_count']}")
            top = result["scores"][:10]
            if top:
                preview = ", ".join(f"{x['bvid']}({x['score']:.1f})" for x in top)
                log(f"Top 打分: {preview}")
        return 0
    except Exception as exc:
        log_exception("视频打分失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
