"""表单字段来源校验重构的确定性测试。

覆盖：共享来源判定、两个入口各自的口径与返回、执行间失败凭据隔离、
递交给重试策略前的中断与续做、以及正常路径返回结构不变。
"""
from __future__ import annotations

import pytest

from app.pipeline import (
    resume_form_field_recovery,
    validate_form_field,
)
from app.services import service_for


def _make_field(source, name="字段A", code="FORM-T1"):
    field_svc = service_for("form_field")
    entry, missing = field_svc.create_entry({
        "字段编号": code,
        "关联模板": "TPL-T",
        "表单名称": "表单T",
        "字段名称": name,
        "取值来源": source,
        "校验规则": "required",
        "填充顺序": "1",
        "失败提示": "字段校验未通过",
    })
    assert not missing
    return entry


def _make_trace(run_code="FLOW-T"):
    trace_svc = service_for("step_trace")
    entry, _ = trace_svc.create_entry({
        "轨迹编号": f"STEP-{run_code}",
        "关联执行": run_code,
        "步骤序号": "1",
        "步骤名称": "填充步骤",
        "输入快照": "{}",
        "输出快照": "{}",
        "耗时毫秒": "100",
        "退出码": "0",
    })
    return entry


@pytest.fixture(autouse=True)
def field_rule():
    """启用一条「字段」范围的异常规则并绑定重试策略。"""
    rule_svc = service_for("exception_rule")
    rule_svc.run_action(2, "启效规则")
    rule = rule_svc.get_entry(2)
    rule["适用范围"] = "字段校验"
    retry_svc = service_for("retry_policy")
    policy = retry_svc.get_entry(2)
    policy["关联规则"] = rule["规则编号"]
    policy["累计重试"] = "0"
    policy["最大次数"] = "5"
    policy["放弃阈值"] = "5"
    return rule


class TestSharedSourceDetermination:
    def test_both_entries_read_the_same_determination(self):
        field_svc = service_for("form_field")
        entry = _make_field("datasource_a")
        determination = field_svc.resolve_source(entry["id"])
        assert determination["source_raw"] == "datasource_a"
        assert determination["has_source"] is True
        assert determination["source_is_name_raw"] is False
        assert determination["source_is_name_stripped"] is False
        # 两个入口都消费同一份快照，不再各自读字段。
        assert field_svc.trial_source_valid(determination) is True
        assert field_svc.collection_source_valid(determination) is True

    def test_trial_keeps_strip_rule_collection_keeps_raw_compare(self):
        field_svc = service_for("form_field")
        entry = _make_field("  字段A  ", name="字段A")
        determination = field_svc.resolve_source(entry["id"])
        # 去空格后等于字段名称：试算判定无效。
        assert field_svc.trial_source_valid(determination) is False
        # 原文带空格且不等于字段名称：采集判定有效（保留原比较口径）。
        assert field_svc.collection_source_valid(determination) is True

    def test_missing_source_fails_both(self):
        field_svc = service_for("form_field")
        entry = _make_field("", name="字段A")
        determination = field_svc.resolve_source(entry["id"])
        assert field_svc.trial_source_valid(determination) is False
        assert field_svc.collection_source_valid(determination) is False


class TestEntryPointBehaviourPreserved:
    def test_trial_return_shape_and_strip_behaviour(self):
        field_svc = service_for("form_field")
        ok_entry = _make_field("datasource_a", name="字段A", code="FORM-T2")
        result = field_svc.validate_field(ok_entry["id"])
        assert result == {"ok": True, "field_id": ok_entry["id"], "valid": True}

        # 去空格后等于字段名称 → 试算失败（原口径）。
        spaced = _make_field("  字段B  ", name="字段B", code="FORM-T3")
        result = field_svc.validate_field(spaced["id"])
        assert result == {"ok": False, "reason": "取值来源无效，校验失败"}

        missing = _make_field("", name="字段C", code="FORM-T4")
        result = field_svc.validate_field(missing["id"])
        assert result == {"ok": False, "reason": "取值来源无效，校验失败"}

    def test_trial_does_not_write_trace_or_evidence(self):
        field_svc = service_for("form_field")
        trace_svc = service_for("step_trace")
        entry = _make_field("", name="字段D", code="FORM-T5")
        trace = _make_trace()
        before = dict(trace)
        field_svc.validate_field(entry["id"])
        # 试算不改变运行轨迹
        assert trace == before
        assert field_svc.source_failure_evidence(field_id=entry["id"]) == []
        assert trace_svc.get_entry(trace["id"])["status"] == "已记录"

    def test_normal_collection_path_returns_original_shape(self):
        entry = _make_field("datasource_ok", name="字段E", code="FORM-T6")
        trace = _make_trace()
        result = validate_form_field(entry["id"], trace["id"], execution_id="EXEC-OK")
        assert result == {"status": "validated", "field_id": entry["id"], "is_valid": True}

    def test_spaced_source_passes_collection_but_fails_trial(self):
        field_svc = service_for("form_field")
        entry = _make_field("  字段F  ", name="字段F", code="FORM-T7")
        trace = _make_trace()
        # 采集按原比较口径：带空格原文 != 字段名称 → 通过，走正常返回。
        result = validate_form_field(entry["id"], trace["id"], execution_id="EXEC-SP")
        assert result == {"status": "validated", "field_id": entry["id"], "is_valid": True}
        # 试算按去空格口径：失败。
        trial = field_svc.validate_field(entry["id"])
        assert trial == {"ok": False, "reason": "取值来源无效，校验失败"}


class TestInterruptAndResume:
    def test_deferred_failure_records_trace_then_resumes_once(self):
        field_svc = service_for("form_field")
        retry_svc = service_for("retry_policy")
        entry = _make_field("", name="字段G", code="FORM-T8")
        trace = _make_trace()

        policy_before = int(retry_svc.get_entry(2)["累计重试"])

        # A：缺来源，写出失败轨迹，停在递交恢复请求之前。
        result = validate_form_field(
            entry["id"], trace["id"], execution_id="EXEC-A", defer_recovery=True)
        assert result["status"] == "pending_recovery"
        assert result["execution_id"] == "EXEC-A"

        stored_trace = service_for("step_trace").get_entry(trace["id"])
        assert stored_trace["status"] == "异常"
        assert stored_trace["执行标识"] == "EXEC-A"
        assert stored_trace["来源判定"]["字段编号"] == "FORM-T8"
        assert stored_trace["失败依据"] == "取值来源无效，校验失败"

        evidence = field_svc.get_source_failure("EXEC-A", entry["id"])
        assert evidence["status"] == "pending"
        assert evidence["attempt"] == 0
        # 尚未递交恢复请求，策略计数没有变化。
        assert int(retry_svc.get_entry(2)["累计重试"]) == policy_before

        resumed = resume_form_field_recovery(entry["id"], execution_id="EXEC-A")
        assert resumed["status"] == "retrying"
        assert resumed["attempt"] == 1
        assert resumed["execution_id"] == "EXEC-A"
        assert int(retry_svc.get_entry(2)["累计重试"]) == policy_before + 1

        # 中断恢复不能重复消耗同一次重试。
        again = resume_form_field_recovery(entry["id"], execution_id="EXEC-A")
        assert again["attempt"] == 1
        assert int(retry_svc.get_entry(2)["累计重试"]) == policy_before + 1
        assert field_svc.get_source_failure("EXEC-A", entry["id"])["status"] == "submitted"

    def test_separate_executions_do_not_share_failure_evidence(self):
        field_svc = service_for("form_field")
        retry_svc = service_for("retry_policy")
        entry = _make_field("", name="字段H", code="FORM-T9")
        trace_a = _make_trace("FLOW-A")
        trace_b = _make_trace("FLOW-B")
        base = int(retry_svc.get_entry(2)["累计重试"])

        # A 失败并暂停在递交之前。
        validate_form_field(entry["id"], trace_a["id"], execution_id="EXEC-A",
                            defer_recovery=True)
        # B 同时提交同字段缺来源，独立走完整恢复。
        result_b = validate_form_field(entry["id"], trace_b["id"], execution_id="EXEC-B")
        assert result_b["status"] == "retrying"
        assert result_b["attempt"] == 1

        # B 的恢复不消耗 A 的凭据。
        assert field_svc.get_source_failure("EXEC-A", entry["id"])["status"] == "pending"
        assert field_svc.get_source_failure("EXEC-B", entry["id"])["status"] == "submitted"

        # A 继续用原输入续做，拿到自己的下一次尝试。
        resumed_a = resume_form_field_recovery(entry["id"], execution_id="EXEC-A")
        assert resumed_a["status"] == "retrying"
        assert resumed_a["attempt"] == 2
        assert resumed_a["execution_id"] == "EXEC-A"
        assert int(retry_svc.get_entry(2)["累计重试"]) == base + 2

        # 各自凭据独立可追查，递交者标记正确。
        evidence_a = field_svc.source_failure_evidence(execution_id="EXEC-A")
        evidence_b = field_svc.source_failure_evidence(execution_id="EXEC-B")
        assert len(evidence_a) == 1 and len(evidence_b) == 1
        assert evidence_a[0]["submitted_by"] == "EXEC-A"
        assert evidence_b[0]["submitted_by"] == "EXEC-B"
        assert evidence_a[0]["trace_ids"] == [trace_a["id"]]
        assert evidence_b[0]["trace_ids"] == [trace_b["id"]]


class TestEvidenceBackedRecovery:
    def test_retry_audit_links_back_to_execution(self):
        entry = _make_field("", name="字段I", code="FORM-T10")
        trace = _make_trace()
        result = validate_form_field(entry["id"], trace["id"], execution_id="EXEC-AUD")
        assert result["status"] == "retrying"
        audit = service_for("audit_trail").get_entry(result["audit_id"])
        assert audit is not None
        assert audit["关联执行"] == "EXEC-AUD"
        assert "EXEC-AUD" in audit["追溯标记"] or "EXEC-AUD" in audit["变更摘要"]
