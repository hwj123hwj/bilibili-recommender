# TODO - 开发进度

## Phase 1: 基础设施
- [x] PostgreSQL + pgvector 容器已运行
- [ ] 创建数据库表（init_recommendation_db.py）
- [ ] 初始化表并测试连接

## Phase 2: 核心功能
- [ ] 实现 bili_followings_updater.py
  - [ ] 使用 bilibili-api 获取关注列表
  - [ ] 同步到 user_followings 表
  - [ ] 检测关注变化
- [ ] 实现 bili_new_video_checker.py
  - [ ] 遍历 user_followings 获取 UP主列表
  - [ ] 获取每个 UP主最新视频
  - [ ] 筛选未入库的新视频
- [ ] 实现 bili_interest_profiler.py
  - [ ] 从 bili_video_contents 分析历史视频
  - [ ] 统计高频标签
  - [ ] 使用 LLM 提取关键词
  - [ ] 更新 user_interest_tags 表

## Phase 3: 推荐引擎
- [ ] 实现 bili_video_scorer.py
  - [ ] 标签匹配评分（40%）
  - [ ] 关键词匹配评分（30%）
  - [ ] 向量相似度评分（20%）
  - [ ] UP主偏好评分（10%）
  - [ ] 生成推荐理由
- [ ] 实现 bili_recommendation_scheduler.py
  - [ ] 定时检查新视频
  - [ ] 调用视频检查器和打分器
  - [ ] 过滤低分视频
  - [ ] 调用飞书推送器

## Phase 4: 展示和文档
- [ ] 实现 bili_recommendation_formatter.py
  - [ ] 读取 recommendation_logs 待展示记录
  - [ ] 格式化推荐消息为自然语言
  - [ ] 更新推荐记录状态
- [ ] 创建 SKILL.md（OpenClaw skill 文档）
- [ ] 测试完整流程
- [ ] 编写使用文档

## 测试
- [ ] 单元测试
- [ ] 集成测试
- [ ] 真实环境测试

## 优化
- [ ] 性能优化
- [ ] 错误处理优化
- [ ] 日志记录完善
