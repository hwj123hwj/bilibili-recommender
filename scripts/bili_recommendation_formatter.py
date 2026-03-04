#!/usr/bin/env python3
"""
读取 recommendation_logs 待处理推荐，并输出为多种格式。

支持格式：
- json
- markdown
- table
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence

from bili_recommender_common import CONFIG, fetch_all_dict, get_db_conn, json_dumps, log, log_exception, normalize_tags


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="格式化推荐结果")
    parser.add_argument("--uid", type=int, default=CONFIG.bili_uid, help="用户 UID")
    parser.add_argument("--status", type=str, default="pending", help="读取状态，默认 pending")
    parser.add_argument("--limit", type=int, default=20, help="最多输出数量")
    parser.add_argument(
        "--format",
        type=str,
        default="markdown",
        choices=["json", "markdown", "table"],
        help="输出格式",
    )
    parser.add_argument("--mark-viewed", action="store_true", help="输出后将 pending 记录标记为 viewed")
    return parser.parse_args()


def _table_exists(conn: Any, table_name: str) -> bool:
    rows = fetch_all_dict(
        conn,
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        LIMIT 1
        """,
        (table_name,),
    )
    return bool(rows)


def load_recommendations(uid: int, status: str, limit: int) -> List[Dict[str, Any]]:
    with get_db_conn() as conn:
        has_new_videos = _table_exists(conn, "bili_new_videos")
        has_contents = _table_exists(conn, "bili_video_contents")

        joins: List[str] = []
        select_parts = [
            "rl.id",
            "rl.uid",
            "rl.bvid",
            "rl.recommended_at",
            "COALESCE(rl.score, 0.0) AS score",
            "COALESCE(rl.score_tags, 0.0) AS score_tags",
            "COALESCE(rl.score_keywords, 0.0) AS score_keywords",
            "COALESCE(rl.score_vector, 0.0) AS score_vector",
            "COALESCE(rl.score_up, 0.0) AS score_up",
            "COALESCE(rl.reason, '') AS reason",
            "COALESCE(rl.status, 'pending') AS status",
        ]

        if has_new_videos:
            joins.append("LEFT JOIN bili_new_videos nv ON nv.bvid = rl.bvid")
            select_parts.extend(
                [
                    "COALESCE(nv.title, '') AS title_nv",
                    "COALESCE(nv.up_name, '') AS up_name_nv",
                    "COALESCE(nv.up_mid, 0) AS up_mid_nv",
                    "nv.tags AS tags_nv",
                    "COALESCE(nv.description, '') AS desc_nv",
                ]
            )
        else:
            select_parts.extend(
                [
                    "'' AS title_nv",
                    "'' AS up_name_nv",
                    "0 AS up_mid_nv",
                    "NULL AS tags_nv",
                    "'' AS desc_nv",
                ]
            )

        if has_contents:
            joins.append("LEFT JOIN bili_video_contents vc ON vc.bvid = rl.bvid")
            select_parts.extend(
                [
                    "COALESCE(vc.title, '') AS title_vc",
                    "COALESCE(vc.up_name, '') AS up_name_vc",
                    "COALESCE(vc.up_mid, 0) AS up_mid_vc",
                    "vc.tags AS tags_vc",
                    "COALESCE(vc.description, COALESCE(vc.content_text, '')) AS desc_vc",
                ]
            )
        else:
            select_parts.extend(
                [
                    "'' AS title_vc",
                    "'' AS up_name_vc",
                    "0 AS up_mid_vc",
                    "NULL AS tags_vc",
                    "'' AS desc_vc",
                ]
            )

        sql = f"""
        SELECT {', '.join(select_parts)}
        FROM recommendation_logs rl
        {' '.join(joins)}
        WHERE rl.uid = %s AND rl.status = %s
        ORDER BY rl.score DESC, rl.recommended_at DESC
        LIMIT %s
        """
        rows = fetch_all_dict(conn, sql, (uid, status, limit))

    results: List[Dict[str, Any]] = []
    for r in rows:
        tags = normalize_tags(r.get("tags_nv")) or normalize_tags(r.get("tags_vc"))
        title = (r.get("title_nv") or r.get("title_vc") or "").strip()
        up_name = (r.get("up_name_nv") or r.get("up_name_vc") or "").strip()
        up_mid = int(r.get("up_mid_nv") or r.get("up_mid_vc") or 0)
        description = (r.get("desc_nv") or r.get("desc_vc") or "").strip()

        results.append(
            {
                "id": int(r["id"]),
                "uid": int(r["uid"]),
                "bvid": str(r["bvid"]),
                "title": title,
                "up_name": up_name,
                "up_mid": up_mid,
                "tags": tags,
                "description": description,
                "score": round(float(r.get("score") or 0.0), 2),
                "score_tags": round(float(r.get("score_tags") or 0.0), 2),
                "score_keywords": round(float(r.get("score_keywords") or 0.0), 2),
                "score_vector": round(float(r.get("score_vector") or 0.0), 2),
                "score_up": round(float(r.get("score_up") or 0.0), 2),
                "reason": str(r.get("reason") or ""),
                "status": str(r.get("status") or "pending"),
                "recommended_at": r.get("recommended_at"),
                "url": f"https://www.bilibili.com/video/{str(r['bvid'])}",
            }
        )

    return results


def mark_as_viewed(ids: Sequence[int]) -> int:
    if not ids:
        return 0
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recommendation_logs
                SET status = 'viewed'
                WHERE id = ANY(%s) AND status = 'pending'
                """,
                (list(ids),),
            )
            return int(cur.rowcount or 0)


def _truncate(text: str, width: int) -> str:
    txt = str(text or "")
    if len(txt) <= width:
        return txt
    if width <= 1:
        return txt[:width]
    return txt[: width - 1] + "…"


def render_table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "暂无待处理推荐。"

    headers = ["#", "BVID", "分数", "UP", "标题", "标签"]
    table_rows: List[List[str]] = []
    for idx, item in enumerate(rows, start=1):
        table_rows.append(
            [
                str(idx),
                item["bvid"],
                f"{item['score']:.1f}",
                _truncate(item.get("up_name") or "-", 16),
                _truncate(item.get("title") or "(无标题)", 40),
                _truncate(",".join(item.get("tags") or []) or "-", 28),
            ]
        )

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row: Iterable[str]) -> str:
        return " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(row))

    sep = "-+-".join("-" * w for w in widths)
    lines = [fmt(headers), sep]
    lines.extend(fmt(r) for r in table_rows)
    return "\n".join(lines)


def render_markdown(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "## B站视频推荐\n\n暂无待处理推荐。"

    lines = [f"## B站视频推荐（{len(rows)} 条）", ""]
    for idx, item in enumerate(rows, start=1):
        lines.append(f"### {idx}. {item.get('title') or '(无标题)'}")
        lines.append(f"- BVID: `{item['bvid']}`")
        lines.append(f"- UP主: {item.get('up_name') or '-'} ({item.get('up_mid') or 0})")
        lines.append(f"- 匹配分: **{item['score']:.1f}/100**")
        lines.append(
            f"- 分项: Tags {item['score_tags']:.1f} / Keywords {item['score_keywords']:.1f} / Vector {item['score_vector']:.1f} / UP {item['score_up']:.1f}"
        )
        lines.append(f"- 标签: {', '.join(item.get('tags') or []) or '-'}")
        lines.append(f"- 理由: {item.get('reason') or '-'}")
        lines.append(f"- 链接: {item['url']}")
        lines.append("")
    return "\n".join(lines).rstrip()


def run(uid: int, status: str, limit: int, output_format: str, mark_viewed: bool) -> Dict[str, Any]:
    rows = load_recommendations(uid=uid, status=status, limit=limit)

    if output_format == "json":
        rendered = json_dumps({"uid": uid, "count": len(rows), "recommendations": rows})
    elif output_format == "table":
        rendered = render_table(rows)
    else:
        rendered = render_markdown(rows)

    viewed_count = 0
    if mark_viewed and status == "pending" and rows:
        viewed_count = mark_as_viewed([r["id"] for r in rows])

    return {
        "uid": uid,
        "status": status,
        "count": len(rows),
        "mark_viewed": mark_viewed,
        "viewed_count": viewed_count,
        "output": rendered,
    }


def main() -> int:
    args = parse_args()
    try:
        result = run(
            uid=args.uid,
            status=args.status,
            limit=args.limit,
            output_format=args.format,
            mark_viewed=args.mark_viewed,
        )
        print(result["output"])
        if args.mark_viewed and args.status == "pending":
            log(f"已标记 viewed 数量: {result['viewed_count']}")
        return 0
    except Exception as exc:
        log_exception("推荐格式化失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
