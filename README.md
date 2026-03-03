# B站视频智能推荐系统 (Bilibili Intelligent Recommender)

## 📋 项目概述

**目标**：为 B站关注的 UP 主新视频提供智能过滤和推荐，避免信息过载。

## 🏗️ 技术架构

### 基础设施
- **数据库**: PostgreSQL + pgvector (Docker 容器运行)
  - Host: 127.0.0.1
  - Port: 5432
  - User: root
  - Password: 15671040800q
  - Database: media_knowledge_base
- **ASR**: SiliconFlow API
- **向量搜索**: llama-index + pgvector
- **LLM**: SiliconFlow / Gemini

### 用户的 B站信息
- **UID**: 1512253857
- **个人主页**: https://space.bilibili.com/1512253857

## 📚 参考资源

1. **核心基础**: https://github.com/hwj123hwj/custom-skills/tree/main/bilibili-toolkit
   - 现有的 B站 toolkit
   - 包含数据库结构、ASR、向量搜索

2. **B站 API**: https://github.com/Nemo2011/bilibili-api
   - B站开源 API 库
   - 用于获取关注列表、历史记录等

3. **本地参考**: `~/.openclaw/workspace/skills/bili-summary/`
   - 已安装的 bili-summary skill
   - 包含字幕提取逻辑

## 🗄️ 数据库表设计

### 现有表（不要修改）
- ✅ `bili_video_contents` - 视频内容
- ✅ `up_users` - UP主信息

### 需要新增的表

```sql
-- 用户关注列表
CREATE TABLE IF NOT EXISTS user_followings (
  id SERIAL PRIMARY KEY,
  uid BIGINT,  -- 用户的B站UID
  up_mid BIGINT NOT NULL,  -- 关注的UP主ID
  followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(uid, up_mid)
);

-- 推荐记录
CREATE TABLE IF NOT EXISTS recommendation_logs (
  id SERIAL PRIMARY KEY,
  bvid VARCHAR(50) NOT NULL UNIQUE,
  recommended_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  score FLOAT DEFAULT 0.0,  -- 匹配分数
  reason TEXT,  -- 推荐理由
  status VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'viewed', 'liked', 'disliked'
  FOREIGN KEY (bvid) REFERENCES bili_video_contents(bvid) ON DELETE CASCADE
);

-- 用户兴趣标签
CREATE TABLE IF NOT EXISTS user_interest_tags (
  id SERIAL PRIMARY KEY,
  tag_name VARCHAR(100) UNIQUE NOT NULL,
  weight FLOAT DEFAULT 1.0,  -- 权重，用于排序
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_recommendation_logs_status ON recommendation_logs(status);
CREATE INDEX IF NOT EXISTS idx_recommendation_logs_score ON recommendation_logs(score DESC);
```

## 🔧 功能模块

### 模块 1: 关注列表管理 (bili_followings_updater.py)

**功能**：
- 使用 bilibili-api 获取当前用户的关注列表
- 同步到 user_followings 表
- 检测关注变化（新增/取消关注）

**API 参考**：
```python
from bilibili_api import user, Credential

credential = Credential(sessdata=SESSDATA, bili_jct=BILI_JCT, buvid3=BUVID3)
u = user.User(uid=YOUR_UID, credential=credential)
followings = await u.get_all_followings()
```

**输出**：
- 更新 user_followings 表
- 打印关注数量统计

---

### 模块 2: 新视频检查 (bili_new_video_checker.py)

**功能**：
- 遍历 user_followings 中的 UP主
- 使用 bilibili-api 获取最新视频列表
- 查询 bili_video_contents 表，筛选未入库的新视频

**API 参考**：
```python
from bilibili_api import user

u = user.User(uid=up_mid, credential=credential)
videos = await u.get_videos(pn=1, ps=30)
```

**输出**：
- 新视频列表（JSON 或数据库临时表）

---

### 模块 3: 兴趣画像建模 (bili_interest_profiler.py)

**功能**：
- 从 bili_video_contents 表分析历史观看/收藏视频
- 统计高频标签（tags 字段）
- 提取关键词（从 content_text 字段）
- 使用 LLM 分析兴趣主题
- 更新 user_interest_tags 表

**实现思路**：
1. 查询最近 N 个月的视频（可配置）
2. 统计 tags 数组中的标签频次
3. 使用 LLM 从 content_text 中提取关键词
4. 按权重排序，写入 user_interest_tags

**输出**：
- 兴趣标签列表（按权重排序）
- 更新 user_interest_tags 表

---

### 模块 4: 视频兴趣匹配 (bili_video_scorer.py)

**功能**：
- 对新视频进行兴趣匹配打分
- 综合评分：
  - 标签匹配分数（40%）
  - 关键词匹配分数（30%）
  - 向量相似度（20%）
  - UP主偏好（10%）
- 生成推荐理由

**实现思路**：
1. 标签匹配：视频标签 vs user_interest_tags
2. 关键词匹配：视频标题/描述 vs 兴趣关键词
3. 向量相似度：视频向量 vs 兴趣向量（从历史视频计算）
4. UP主偏好：该UP主的历史视频是否被喜欢

**输出**：
- 视频评分列表（BVID + 分数 + 匹配原因）
- 生成推荐理由（LLM）

---

### 模块 5: 定时推荐引擎 (bili_recommendation_scheduler.py)

**功能**：
- 定时检查新视频（可通过 cron 或内置调度器）
- 调用 bili_new_video_checker.py 获取新视频
- 调用 bili_video_scorer.py 打分排序
- 过滤低分视频（可配置阈值）
- 调用 bili_feishu_notifier.py 推送

**配置**：
```python
CHECK_INTERVAL = 3600  # 每小时检查一次
MIN_SCORE_THRESHOLD = 0.6  # 最低推荐分数
MAX_RECOMMENDATIONS_PER_RUN = 10  # 每次最多推荐10个
```

**输出**：
- 更新 recommendation_logs 表
- 触发飞书推送

---

### 模块 6: 飞书推送 (bili_feishu_notifier.py)

**功能**：
- 格式化推荐结果
- 通过飞书 Webhook 或 API 推送
- 消息格式：标题、UP主、标签、推荐理由、视频链接

**实现思路**：
1. 读取 recommendation_logs 中 status='pending' 的记录
2. 关联 bili_video_contents 和 up_users 表获取详细信息
3. 格式化为飞书富文本消息
4. 调用飞书 API 推送

**消息格式示例**：
```
🎬 B站视频推荐

【1】视频标题
UP主: XXX
标签: AI, 技术, 编程
匹配度: 8.5/10
推荐理由: 该视频讲解了最新的 AI 技术，符合你对技术内容的兴趣...
链接: https://www.bilibili.com/video/BVxxx
```

## 📁 项目结构

```
bilibili-toolkit/
├── SKILL.md  # 更新文档
├── scripts/
│   ├── bili_collect_and_export.py  # 现有（不要修改）
│   ├── bili_kb_llama.py  # 现有（不要修改）
│   ├── bili_search_llama.py  # 现有（不要修改）
│   ├── bili_up_summarizer.py  # 现有（不要修改）
│   ├── bili_followings_updater.py  # ⭐ 新增
│   ├── bili_new_video_checker.py  # ⭐ 新增
│   ├── bili_interest_profiler.py  # ⭐ 新增
│   ├── bili_video_scorer.py  # ⭐ 新增
│   ├── bili_recommendation_scheduler.py  # ⭐ 新增
│   ├── bili_feishu_notifier.py  # ⭐ 新增
│   └── init_recommendation_db.py  # ⭐ 新增（初始化数据库表）
└── secrets.json  # 现有（不要修改）
```

## 🔑 配置要求

### 环境变量（已有）

```bash
# 数据库配置（已设置）
DB_USER=root
DB_PASSWORD=15671040800q
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=media_knowledge_base

# B站 API（已有）
BILIBILI_COOKIE=<你的B站Cookie>

# SiliconFlow API（已有）
SILICONFLOW_API_KEY=<你的API Key>
```

### 新增配置（需要添加）

```bash
# 飞书推送（需要配置）
FEISHU_WEBHOOK_URL=<你的飞书Webhook URL>
# 或使用 Feishu Open API
FEISHU_APP_ID=<飞书应用ID>
FEISHU_APP_SECRET=<飞书应用密钥>

# 推荐系统配置
YOUR_BILIBILI_UID=1512253857
CHECK_INTERVAL_SECONDS=3600  # 检查间隔（秒）
MIN_SCORE_THRESHOLD=0.6  # 最低推荐分数
MAX_RECOMMENDATIONS=10  # 每次最多推荐数量
```

## 📝 实现顺序

### Phase 1: 基础设施（优先）
1. ✅ PostgreSQL + pgvector (已完成)
2. ⏳ 创建数据库表（init_recommendation_db.py）
3. ⏳ 初始化表

### Phase 2: 核心功能
1. ⏳ 实现 bili_followings_updater.py
2. ⏳ 实现 bili_new_video_checker.py
3. ⏳ 实现 bili_interest_profiler.py

### Phase 3: 推荐引擎
1. ⏳ 实现 bili_video_scorer.py
2. ⏳ 实现 bili_recommendation_scheduler.py

### Phase 4: 推送
1. ⏳ 实现 bili_feishu_notifier.py
2. ⏳ 更新 SKILL.md 文档

## ⚠️ 注意事项

1. **不要修改现有代码**：bili_collect_and_export.py 等现有脚本保持不变
2. **复用现有功能**：ASR、向量搜索等直接调用已有脚本或模块
3. **数据库事务**：确保插入/更新操作使用事务，避免数据不一致
4. **错误处理**：API 调用失败要有重试机制和日志记录
5. **配置灵活**：所有阈值和参数应可通过环境变量配置
6. **飞书推送**：使用 OpenClaw 的 message 工具（feishu channel），不需要额外 Webhook

## 🎯 验收标准

1. ✅ 能正确获取关注列表并同步到数据库
2. ✅ 能检测到新视频
3. ✅ 兴趣画像能反映用户的观看偏好
4. ✅ 推荐分数合理，符合用户兴趣
5. ✅ 能通过飞书成功推送推荐结果
6. ✅ 推荐历史可查询，支持反馈（喜欢/不感兴趣）

## 📄 许可证

MIT License
