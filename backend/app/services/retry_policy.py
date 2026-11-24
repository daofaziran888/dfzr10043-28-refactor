"""重试策略服务：退避参数、放弃阈值与重试执行管理。

维护异常处置对应的重试策略，含退避基数、倍数和抖动区间，
达到放弃阈值后抑制并为下游写入审计轨迹。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class RetryPolicyService(BaseModuleService):
    MODULE = "retry_policy"
    ENTITY = "重试策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "策略编号", "关联规则", "最大次数", "退避基数", "退避倍数",
        "抖动区间", "放弃阈值", "累计重试",
    ]
    REQUIRED_FIELDS = ["策略编号", "关联规则", "最大次数"]
    STATUS_ORDER = ["草稿", "执行中", "已放弃", "已停用"]
    ACTION_RULES = {"启用策略": "执行中", "标记放弃": "已放弃", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def should_abandon(self, policy_id: int) -> bool:
        """判断重试是否应放弃：累计重试>=最大次数或达到放弃阈值。"""
        entry = self.get_entry(policy_id)
        if entry is None:
            return True
        max_attempts = int(entry.get("最大次数", "3") or "3")
        current = int(entry.get("累计重试", "0") or "0")
        threshold = int(entry.get("放弃阈值", str(max_attempts)) or str(max_attempts))
        return current >= threshold or current >= max_attempts

    def calculate_backoff(self, policy_id: int, attempt: int) -> int:
        """计算退避等待毫秒数，含指数退避与抖动。"""
        entry = self.get_entry(policy_id)
        if entry is None:
            return 1000
        base = int(entry.get("退避基数", "500") or "500")
        multiplier = int(entry.get("退避倍数", "2") or "2")
        jitter_range = entry.get("抖动区间", "200") or "200"
        jitter = int(jitter_range)
        import random
        return base * (multiplier ** (attempt - 1)) + random.randint(0, jitter)

    def increment_retry(self, policy_id: int) -> dict[str, Any]:
        """递增累计重试次数。"""
        entry = self.get_entry(policy_id)
        if entry is None:
            return {"ok": False, "reason": "策略不存在"}
        current = int(entry.get("累计重试", "0") or "0")
        entry["累计重试"] = str(current + 1)
        return {"ok": True, "policy_id": policy_id, "total_retries": current + 1}

    # ── 按失败凭据接续未完成的失败处置 ─────────────────────────

    def continue_failure_recovery(
        self,
        evidence: dict[str, Any],
        policy_id: int,
        *,
        signature: str = "",
        execution_id: str | None = None,
    ) -> dict[str, Any]:
        """用失败凭据接续重试处置；同一条凭据只消耗一次重试。

        凭据已处于 submitted/abandoned/submitting 时直接复用既有结果，
        中断后重复续做不会再次递增「累计重试」。
        """
        from app.services import service_for

        execution_id = execution_id or str(evidence.get("execution_id", ""))
        state = evidence.get("status")
        if state in ("submitting", "submitted", "abandoned"):
            if evidence.get("result"):
                return dict(evidence["result"])
            return {
                "status": "abandoned" if state == "abandoned" else "retrying",
                "policy_id": policy_id,
                "attempt": int(evidence.get("attempt", 0) or 0),
                "retry_total": int(evidence.get("retry_total", 0) or 0),
                "execution_id": execution_id,
                "resumed": True,
            }

        entry = self.get_entry(policy_id)
        if entry is None:
            return {"status": "no_policy", "reason": "重试策略不存在"}

        max_attempts = int(entry.get("最大次数", "3") or "3")
        threshold = int(entry.get("放弃阈值", str(max_attempts)) or str(max_attempts))
        current = int(entry.get("累计重试", "0") or "0")
        policy_code = entry.get("策略编号", "")

        # 已达放弃阈值：只标记放弃，不消耗新的重试
        if current >= threshold or current >= max_attempts:
            evidence["status"] = "submitting"
            self.run_action(policy_id, "标记放弃")
            result = {
                "status": "abandoned",
                "policy_id": policy_id,
                "max_attempts": max_attempts,
                "retry_total": current,
                "execution_id": execution_id,
            }
            evidence["status"] = "abandoned"
            evidence["result"] = dict(result)
            evidence["submitted_by"] = execution_id
            audit_svc = service_for("audit_trail")
            audit_result, _ = audit_svc.create_entry({
                "条目编号": f"AUDI-RCV-{policy_id}-{evidence.get('field_id')}",
                "操作对象": f"重试策略.{policy_code}",
                "操作类型": "重试放弃",
                "操作者标识": "system",
                "操作时间": _now_iso(),
                "变更摘要": (
                    f"异常{signature}经{max_attempts}次重试后放弃，"
                    f"由执行{execution_id}的失败凭据接续"
                ),
                "关联执行": execution_id,
                "追溯标记": f"recovery-abandon-{execution_id}-{evidence.get('field_id')}",
            })
            result["audit_id"] = audit_result["id"]
            evidence["result"] = dict(result)
            return result

        # 消耗一次重试：先落凭据状态，保证中断后不会再次消耗
        evidence["status"] = "submitting"
        incremented = self.increment_retry(policy_id)
        attempt = int(incremented.get("total_retries", current + 1))
        evidence["attempt"] = attempt
        evidence["retry_total"] = attempt
        self.run_action(policy_id, "启用策略")
        result = {
            "status": "retrying",
            "policy_id": policy_id,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "retry_total": attempt,
            "execution_id": execution_id,
        }
        evidence["status"] = "submitted"
        evidence["result"] = dict(result)
        evidence["submitted_by"] = execution_id

        audit_svc = service_for("audit_trail")
        audit_result, _ = audit_svc.create_entry({
            "条目编号": f"AUDI-RCV-{policy_id}-{evidence.get('field_id')}-A{attempt}",
            "操作对象": f"重试策略.{policy_code}",
            "操作类型": "重试执行",
            "操作者标识": "system",
            "操作时间": _now_iso(),
            "变更摘要": (
                f"异常{signature}第{attempt}/{max_attempts}次重试，"
                f"由执行{execution_id}的失败凭据接续"
            ),
            "关联执行": execution_id,
            "追溯标记": f"recovery-attempt-{execution_id}-{evidence.get('field_id')}",
        })
        result["audit_id"] = audit_result["id"]
        evidence["result"] = dict(result)
        return result