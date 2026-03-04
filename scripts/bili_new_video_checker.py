#!/usr/bin/env python3
"""
检查关注 UP 主是否发布新视频，并写入 bili_new_videos 缓存表。

流程：
1. 从 user_followings 读取关注 UP 主列表
2. 调用 bilibili_api 获取每个 UP 主最近视频
3. 与 bili_video_contents 对比，筛选未入库的视频
4. 写入/更新 bili_new_videos
"""

from __future__ import annotations

import argparse
import asyncio
import http.cookies
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from bilibili_api import Credential, user

from bili_recommender_common import CONFIG, get_db_conn, json_dumps, log, log_exception, normalize_tags


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查新视频")
    parser.add_argument("--uid", type=int, default=CONFIG.bili_uid, help="用户 UID")
    parser.add_argument("--limit-per-up", type=int, default=20, help="每个 UP 拉取视频数量")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    parser.add_argument("--dry-run", action="store_true", help="只输出结果，不入库")
    return parser.parse_args()


def build_credential_from_cookie(cookie_text: str) -> Optional[Credential]:
    if not cookie_text:
        return None
    jar = http.cookies.SimpleCookie()
    try:
        jar.load(cookie_text)
    except Exception:
        return None

    sessdata = jar.get("SESSDATA").value if jar.get("SESSDATA") else None
    bili_jct = jar.get("bili_jct").value if jar.get("bili_jct") else None
    buvid3 = jar.get("buvid3").value if jar.get("buvid3") else None
    dedeuserid = jar.get("DedeUserID").value if jar.get("DedeUserID") else None
    if not sessdata:
        return None

    return Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3, dedeuserid=dedeuserid)


def load_followings(uid: int) -> List[Dict[str, Any]]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT up_mid, COALESCE(up_name, '') AS up_name FROM user_followings WHERE uid = %s ORDER BY up_mid",
                (uid,),
            )
            return [{"up_mid": int(r[0]), "up_name": str(r[1])} for r in cur.fetchall()]


async def fetch_up_videos(up_mid: int, credential: Credential, limit: int) -> List[Dict[str, Any]]:
    """拉取单个 UP 的最近视频，并兼容不同 bilibili_api 返回结构。"""
    u = user.User(uid=up_mid, credential=credential)
    data = await u.get_videos(pn=1, ps=limit)

    if isinstance(data, dict):
        # 常见结构：{"list": {"vlist": [...]}}
        if isinstance(data.get("list"), dict) and isinstance(data["list"].get("vlist"), list):
            return data["list"]["vlist"]
        if isinstance(data.get("vlist"), list):
            return data["vlist"]
        if isinstance(data.get("list"), list):
            return data["list"]
    if isinstance(data, list):
        return data
    return []


def normalize_video_item(raw: Dict[str, Any], up_mid: int, up_name: str) -> Dict[str, Any]:
    """标准化视频字段，避免源字段名差异影响后续逻辑。"""
    bvid = raw.get("bvid") or raw.get("id")
    if not bvid:
        raise ValueError(f"视频数据缺少 bvid: {raw}")

    title = str(raw.get("title") or "").strip()
    desc = str(raw.get("description") or raw.get("desc") or "").strip()

    # B站接口中发布时间常见为秒级时间戳
    pub = raw.get("created") or raw.get("pubdate") or raw.get("ctime")
    pubdate = None
    if isinstance(pub, (int, float)):
        pubdate = datetime.fromtimestamp(pub)

    duration = raw.get("length") or raw.get("duration")
    if isinstance(duration, str) and ":" in duration:
        try:
            parts = [int(x) for x in duration.split(":")]
            if len(parts) == 2:
                duration = parts[0] * 60 + parts[1]
            elif len(parts) == 3:
                duration = parts[0] * 3600 + parts[1] * 60 + parts[2]
        except Exception:
            duration = None

    tags = normalize_tags(raw.get("tags") or raw.get("tag"))

    return {
        "bvid": str(bvid),
        "title": title,
        "up_mid": int(up_mid),
        "up_name": up_name,
        "pubdate": pubdate,
        "duration": int(duration) if isinstance(duration, (int, float)) else None,
        "tags": tags,
        "description": desc,
    }


def filter_new_videos(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """过滤出不在 bili_video_contents 中的视频。"""
    if not candidates:
        return []

    bvids = [item["bvid"] for item in candidates]
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bvid FROM bili_video_contents WHERE bvid = ANY(%s)", (bvids,))
            existed = {row[0] for row in cur.fetchall()}

    return [item for item in candidates if item["bvid"] not in existed]


def upsert_new_videos(videos: List[Dict[str, Any]]) -> None:
    if not videos:
        return

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for v in videos:
                cur.execute(
                    """
                    INSERT INTO bili_new_videos(
                        bvid, title, up_mid, up_name, pubdate, duration, tags, description, content_text, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (bvid)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        up_mid = EXCLUDED.up_mid,
                        up_name = EXCLUDED.up_name,
                        pubdate = EXCLUDED.pubdate,
                        duration = EXCLUDED.duration,
                        tags = EXCLUDED.tags,
                        description = EXCLUDED.description,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        v["bvid"],
                        v["title"],
                        v["up_mid"],
                        v["up_name"],
                        v["pubdate"],
                        v["duration"],
                        v["tags"],
                        v["description"],
                        v["description"],
                    ),
                )


async def run(uid: int, limit_per_up: int, dry_run: bool = False) -> Dict[str, Any]:
    credential = build_credential_from_cookie(CONFIG.bili_cookie)
    if credential is None:
        raise RuntimeError("缺少有效 BILIBILI_COOKIE，至少需要包含 SESSDATA")

    followings = load_followings(uid)
    log(f"读取到关注 UP 数量: {len(followings)}")

    all_candidates: List[Dict[str, Any]] = []
    failed_ups: List[Dict[str, Any]] = []

    for up in followings:
        up_mid = up["up_mid"]
        up_name = up["up_name"]
        try:
            raw_videos = await fetch_up_videos(up_mid=up_mid, credential=credential, limit=limit_per_up)
            for raw in raw_videos:
                try:
                    all_candidates.append(normalize_video_item(raw, up_mid=up_mid, up_name=up_name))
                except Exception:
                    continue
        except Exception as exc:
            failed_ups.append({"up_mid": up_mid, "up_name": up_name, "error": str(exc)})

    # 按 bvid 去重
    dedup_map = {item["bvid"]: item for item in all_candidates}
    dedup_candidates = list(dedup_map.values())
    new_videos = filter_new_videos(dedup_candidates)

    if not dry_run:
        upsert_new_videos(new_videos)

    return {
        "uid": uid,
        "followings": len(followings),
        "checked_video_count": len(dedup_candidates),
        "new_video_count": len(new_videos),
        "failed_up_count": len(failed_ups),
        "failed_ups": failed_ups,
        "new_videos": new_videos,
    }


def main() -> int:
    args = parse_args()
    try:
        result = asyncio.run(run(uid=args.uid, limit_per_up=args.limit_per_up, dry_run=args.dry_run))
        if args.json:
            print(json_dumps(result))
        else:
            log(f"检查视频总数: {result['checked_video_count']}")
            log(f"发现新视频数: {result['new_video_count']}")
            log(f"拉取失败 UP 数: {result['failed_up_count']}")
        return 0
    except Exception as exc:
        log_exception("检查新视频失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
