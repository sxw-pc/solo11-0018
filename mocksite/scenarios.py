"""Deterministic mock-site scenarios.

No wall clock is consulted.  Every page is a pure function of (step, seed),
which makes demos and tests byte-for-byte reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock

SCENARIO_SEED = 20260101
MAX_STEP = 4
BASE_LAST_MODIFIED = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

PAGE_PATHS = {
    "announcements": "/announcements",
    "docs": "/docs",
    "pricing": "/pricing",
    "noisy": "/noisy",
}
PAGE_CHANGED_AT = {"announcements": 3, "docs": 1, "pricing": 0, "noisy": 2}

DOC_PARAGRAPHS = [
    "欢迎阅读本地部署手册。本手册只使用示例域名和占位数据。",
    "安装步骤：创建虚拟环境，安装依赖，然后运行检查命令。",
    "配置说明：监视清单由 name、url 和 interval_seconds 三个必填字段组成。",
    "抓取说明：本工具仅允许访问本机白名单地址，并使用条件请求减少开销。",
    "归一化说明：脚本、样式和模板注释会在比较前移除。",
    "差异说明：文本按语义块对齐，并合并相邻的变更块。",
    "分级说明：重大变更、次要变更和噪声变更分别记录。",
    "告警说明：告警仅写入本地台账与 JSONL outbox，不发送到外部。",
    "保留说明：每个监视项保留最近若干版本，重大版本永久保留。",
    "排障说明：先运行 check-config，再查看只读状态页。",
]
CHANGED_DOC_PARAGRAPH = (
    "归一化说明（更新）：脚本、样式、模板注释与动态计数器会在比较前移除，"
    "并记录归一化器版本。"
)

ANNOUNCEMENT_BASE = "本月服务时间保持不变，维护窗口为每周日 02:00-04:00。"
ANNOUNCEMENT_CHANGED = "【重要】计费规则将于本月底调整，请管理员提前核对配额。"

PRICE_ROWS = (
    ("基础版", "99", "5 个页面"),
    ("团队版", "299", "20 个页面"),
    ("企业版", "899", "100 个页面"),
    ("本地版", "0", "仅本机"),
)


@dataclass
class ScenarioState:
    step: int = 0
    seed: int = SCENARIO_SEED
    lock: RLock = field(default_factory=RLock, repr=False)

    def set_step(self, step: int) -> int:
        if not isinstance(step, int) or not 0 <= step <= MAX_STEP:
            raise ValueError(f"step must be an integer in 0..{MAX_STEP}")
        with self.lock:
            self.step = step
            return self.step

    def get_step(self) -> int:
        with self.lock:
            return self.step


def context_for(page: str, step: int, seed: int = SCENARIO_SEED) -> dict[str, object]:
    if page == "announcements":
        notice = ANNOUNCEMENT_CHANGED if step >= 3 else ANNOUNCEMENT_BASE
        return {
            "notice": notice,
            "window": "每周日 02:00-04:00（UTC）",
            "contact": "由本地运维小组维护，不对外提供联系方式。",
        }
    if page == "docs":
        paragraphs = list(DOC_PARAGRAPHS)
        if step >= 1:
            paragraphs[4] = CHANGED_DOC_PARAGRAPH
        return {"paragraphs": paragraphs, "seed": seed}
    if page == "pricing":
        return {"rows": PRICE_ROWS}
    if page == "noisy":
        return {
            "counter": 731 if step >= 2 else 128,
            "timestamp": "2026-01-01 00:00:00",
            "session": f"session-{seed % 997:03d}",
        }
    raise KeyError(f"unknown mock page: {page}")


def last_modified_for(page: str, step: int) -> datetime:
    changed_at = PAGE_CHANGED_AT[page]
    applied = changed_at if step >= changed_at else 0
    return BASE_LAST_MODIFIED.replace(hour=applied)


def page_version(page: str, step: int) -> int:
    return PAGE_CHANGED_AT[page] if step >= PAGE_CHANGED_AT[page] else 0
