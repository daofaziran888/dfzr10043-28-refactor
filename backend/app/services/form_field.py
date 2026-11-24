"""表单字段服务：取值绑定、来源判定、校验规则和填充顺序管理。

维护流程中表单填充字段的取值来源、校验规则和填充顺序，
校验失败时阻断该步骤。支持重试校验和按模板筛选。

来源读取只有一处（``resolve_source``）：独立试算和实际采集都消费同一份
来源数据判定，但各自保留原有口径——
* 独立试算按去空格后的来源与字段名称比较；
* 实际采集按来源原文与字段名称比较。

失败凭据按「执行标识 + 字段」隔离保存：试算不写凭据、不碰步骤轨迹；
不同执行之间不共享失败凭据；同一凭据的恢复只消耗一次重试。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store

SOURCE_FIELD = "取值来源"
SOURCE_FAILURE_REASON = "取值来源无效，校验失败"


class FormFieldService(BaseModuleService):
    MODULE = "form_field"
    ENTITY = "填充字段"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "字段编号", "关联模板", "表单名称", "字段名称", "取值来源",
        "校验规则", "填充顺序", "失败提示",
    ]
    REQUIRED_FIELDS = ["字段编号", "关联模板", "字段名称"]
    STATUS_ORDER = ["草稿", "已绑定", "校验失败", "已停用"]
    ACTION_RULES = {"绑定取值": "已绑定", "重新校验": "校验失败", "停用字段": "已停用"}
    NEGATIVE_ACTIONS = ["停用字段"]

    # 失败凭据的辅助命名空间；随 store.reset() 一起清空。
    EVIDENCE_NAMESPACE = "form_field_source_evidence"

    # ── 共享来源数据判定：两个入口唯一的来源读取点 ──────────────

    def resolve_source(self, field_id: int | str | dict[str, Any]) -> dict[str, Any] | None:
        """读取字段的取值来源并给出一份两边都能消费的判定快照。

        只做数据读取与去空格派生，不做入口相关的比较结论，
        避免把试算口径和采集口径混在一起。
        """
        if isinstance(field_id, dict):
            entry = field_id
            field_id = entry.get("id", 0)
        else:
            entry = self.get_entry(int(field_id))
        if entry is None:
            return None
        raw = entry.get(SOURCE_FIELD, "")
        name = entry.get("字段名称", "")
        stripped = str(raw).strip()
        return {
            "field_id": field_id,
            "字段编号": entry.get("字段编号", ""),
            "字段名称": name,
            "source_raw": raw,
            "source_stripped": stripped,
            "has_source": bool(raw),
            "source_is_name_raw": raw == name,
            "source_is_name_stripped": stripped == str(name),
        }

    @staticmethod
    def trial_source_valid(determination: dict[str, Any]) -> bool:
        """独立试算口径：原文非空，且去空格后的来源不等于字段名称。"""
        return bool(determination["has_source"]) and not determination["source_is_name_stripped"]

    @staticmethod
    def collection_source_valid(determination: dict[str, Any]) -> bool:
        """实际采集口径：原文非空，且来源原文不等于字段名称（不去空格）。"""
        return bool(determination["has_source"]) and not determination["source_is_name_raw"]

    # ── 独立试算入口（行为与返回结构保持原样） ─────────────────

    def validate_field(self, field_id: int, actual_value: str = "") -> dict[str, Any]:
        """校验表单字段：取值来源非空则通过，否则标记校验失败。

        试算只改变字段自身状态，不写步骤轨迹、不产生失败凭据。
        """
        determination = self.resolve_source(field_id)
        if determination is None:
            return {"ok": False, "reason": "字段不存在"}
        if not self.trial_source_valid(determination):
            self.run_action(field_id, "重新校验")
            return {"ok": False, "reason": SOURCE_FAILURE_REASON}
        self.run_action(field_id, "绑定取值")
        return {"ok": True, "field_id": field_id, "valid": True}

    def get_fields_by_template(self, template_code: str) -> list[dict[str, Any]]:
        """按关联模板获取表单字段列表。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("关联模板") == template_code]

    # ── 失败凭据：按执行标识隔离，供重试策略接续 ──────────────

    @classmethod
    def evidence_key(cls, execution_id: str, field_id: int) -> str:
        """失败凭据键：同一执行 + 同一字段唯一；不同执行天然隔离。"""
        return f"{execution_id}|{field_id}"

    def _evidence_store(self) -> dict[str, dict[str, Any]]:
        return store.aux(self.EVIDENCE_NAMESPACE)

    def record_source_failure(
        self,
        field_id: int,
        *,
        execution_id: str,
        trace_id: int | None = None,
        entry: str = "采集",
        determination: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """记下该执行的缺来源失败凭据；重复记录复用同一条，不重复计数。"""
        determination = determination or self.resolve_source(field_id)
        if determination is None:
            return {"ok": False, "reason": "字段不存在"}
        key = self.evidence_key(execution_id, field_id)
        records = self._evidence_store()
        record = records.get(key)
        if record is None:
            record = {
                "evidence_key": key,
                "execution_id": execution_id,
                "field_id": field_id,
                "字段编号": determination.get("字段编号", ""),
                "entry": entry,
                "reason": SOURCE_FAILURE_REASON,
                "basis": determination,
                "status": "pending",
                "attempt": 0,
                "retry_total": 0,
                "trace_ids": [],
                "result": None,
                "submitted_by": None,
            }
            records[key] = record
        if trace_id is not None and trace_id not in record["trace_ids"]:
            record["trace_ids"].append(trace_id)
        return record

    def get_source_failure(self, execution_id: str, field_id: int) -> dict[str, Any] | None:
        return self._evidence_store().get(self.evidence_key(execution_id, field_id))

    def source_failure_evidence(
        self,
        *,
        execution_id: str | None = None,
        field_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """按执行或字段追查失败凭据，B 的失败不会混到 A 的证据里。"""
        records = list(self._evidence_store().values())
        if execution_id is not None:
            records = [r for r in records if r["execution_id"] == execution_id]
        if field_id is not None:
            records = [r for r in records if r["field_id"] == field_id]
        return records

    def complete_source_failure(
        self,
        execution_id: str,
        field_id: int,
        result: dict[str, Any],
    ) -> dict[str, Any] | None:
        """把重试结果写回凭据；已递交过的凭据原样返回，不重复消耗重试。"""
        record = self.get_source_failure(execution_id, field_id)
        if record is None:
            return None
        if record["status"] in ("submitted", "abandoned"):
            return record
        status = str(result.get("status", ""))
        if status not in ("retrying", "abandoned"):
            return record
        record["status"] = "submitted" if status == "retrying" else "abandoned"
        record["result"] = dict(result)
        record["attempt"] = int(result.get("attempt", record["attempt"]) or 0)
        record["retry_total"] = int(result.get("retry_total", record["retry_total"]) or 0)
        record["submitted_by"] = execution_id
        return record
