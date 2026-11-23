"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import ops, resource

# ops 必须先注册：/api/ops/... 是固定前缀，不能落进 /api/{module} 的动态路径。
ROUTERS = [ops, resource]
