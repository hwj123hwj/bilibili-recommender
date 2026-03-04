#!/usr/bin/env python3
"""
同步 B站关注列表到数据库。

功能：
1. 调用 bilibili_api 拉取指定 UID 的关注列表
2. 同步写入 user_followings 表
3. 识别新增关注和取消关注
4. 输出统计信息（可选 JSON）

依赖环境变量：
- BILIBILI_COOKIE: 完整 cookie 字符串
"""

from __future__ import annotations

import argparse
import asyncio
import http.cookies
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from bilibili_api import Credential, user

from bili_recommender_common import CONFIG, get_db_conn, json_dumps, log, log_exception


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="更新 B站关注列表")
    parser.add_argument("--uid", type=int, default=CONFIG.bili_uid, help="目标用户 UID")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出结果")
    parser.add_argument("--dry-run", action="store_true", help="只计算变更，不写入数据库")
    return parser.parse_args()


def build_credential_from_cookie(cookie_text: str) -> Optional[Credential]:
    """从完整 Cookie 字符串解析 Credential。"""
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

    return Credential(
        sessdata=sessdata,
        bili_jct=bili_jct,
        buvid3=buvid3,
        dedeuserid=dedeuserid,
    )


async def fetch_followings(uid: int, credential: Optional[Credential]) -> List[Dict[str, Any]]:
    """
    拉取完整关注列表。

    优先使用 get_all_followings（若版本支持）。
    若不可用则退化到分页请求。
    """
    u = user.User(uid=uid, credential=credential)

    if hasattr(u, "get_all_followings"):
        data = await u.get_all_followings()
        if isinstance(data, list):
            return data

    # 兼容路径：分页拉取
    all_items: List[Dict[str, Any]] = []
    page = 1
    page_size = 50
    while True:
        if not hasattr(u, "get_followings"):
            raise RuntimeError("当前 bilibili_api 版本不支持 get_followings/get_all_followings")

        resp = await u.get_followings(pn=page, ps=page_size)
        if isinstance(resp, dict):
            items = resp.get("list") or resp.get("items") or []
        else:
            items = resp or []

        if not items:
            break

        all_items.extend(items)
        if len(items) < page_size:
            break
        page += 1

    return all_items


def normalize_following_item(item: Dict[str, Any]) -> Tuple[int, str]:
    """提取关注项中的 UP 主 ID 与昵称。"""
    up_mid = item.get("mid") or item.get("uid") or item.get("id")
    up_name = item.get("uname") or item.get("name") or ""
    if up_mid is None:
        raise ValueError(f"关注项缺少 mid/uid: {item}")
    return int(up_mid), str(up_name or "")


def load_db_followings(uid: int) -> Dict[int, str]:
    """读取数据库中已有关注列表。"""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT up_mid, COALESCE(up_name, '') AS up_name FROM user_followings WHERE uid = %s", (uid,))
            return {int(row[0]): str(row[1]) for row in cur.fetchall()}


def apply_following_changes(
    uid: int,
    current: Dict[int, str],
    db_existing: Dict[int, str],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """将关注差异同步到数据库。"""
    current_ids: Set[int] = set(current.keys())
    db_ids: Set[int] = set(db_existing.keys())

    added_ids = sorted(current_ids - db_ids)
    removed_ids = sorted(db_ids - current_ids)
    kept_ids = sorted(current_ids & db_ids)

    renamed_ids = [mid for mid in kept_ids if (current.get(mid, "") != db_existing.get(mid, ""))]

    if not dry_run:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                # 使用 upsert 处理新增与名称变更
                for mid, name in current.items():
                    cur.execute(
                        """
                        INSERT INTO user_followings(uid, up_mid, up_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (uid, up_mid)
                        DO UPDATE SET up_name = EXCLUDED.up_name, updated_at = CURRENT_TIMESTAMP
                        """,
                        (uid, mid, name),
                    )

                if removed_ids:
                    cur.execute(
                        "DELETE FROM user_followings WHERE uid = %s AND up_mid = ANY(%s)",
                        (uid, removed_ids),
                    )

    return {
        "uid": uid,
        "total_followings": len(current),
        "added_count": len(added_ids),
        "removed_count": len(removed_ids),
        "renamed_count": len(renamed_ids),
        "added": [{"up_mid": mid, "up_name": current.get(mid, "")} for mid in added_ids],
        "removed": [{"up_mid": mid, "up_name": db_existing.get(mid, "")} for mid in removed_ids],
    }


async def run(uid: int, dry_run: bool = False) -> Dict[str, Any]:
    credential = build_credential_from_cookie(CONFIG.bili_cookie)
    if credential is None:
        raise RuntimeError("缺少有效 BILIBILI_COOKIE，至少需要包含 SESSDATA")

    log(f"开始拉取 UID={uid} 的关注列表")
    raw_followings = await fetch_followings(uid, credential)
    current: Dict[int, str] = {}

    for item in raw_followings:
        try:
            mid, name = normalize_following_item(item)
            current[mid] = name
        except Exception:
            # 单条解析失败不应中断全量同步
            continue

    db_existing = load_db_followings(uid)
    result = apply_following_changes(uid=uid, current=current, db_existing=db_existing, dry_run=dry_run)
    return result


def main() -> int:
    args = parse_args()
    try:
        result = asyncio.run(run(uid=args.uid, dry_run=args.dry_run))
        if args.json:
            print(json_dumps(result))
        else:
            log(f"关注总数: {result['total_followings']}")
            log(f"新增关注: {result['added_count']}")
            log(f"取消关注: {result['removed_count']}")
            log(f"昵称变化: {result['renamed_count']}")
        return 0
    except Exception as exc:
        log_exception("更新关注列表失败", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
