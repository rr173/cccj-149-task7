"""reg_inquiry 测试公共夹具。"""
import shutil
import tempfile
import unittest
from datetime import datetime, timezone

from reg_inquiry import InquiryOrchestrator
from reg_inquiry.transport import RegulatorTransport
from reg_inquiry.util import FixedClock

START = datetime(2026, 10, 1, 9, 0, 0, tzinfo=timezone.utc)
DEADLINE = "2026-10-20T00:00:00+00:00"


class InquiryTestBase(unittest.TestCase):
    """每个用例一个独立目录（业务库 + 网关库物理分离）。"""

    fail_times = 0
    reject = False

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="ri-test-")
        self.clock = FixedClock(START)
        self.gw = RegulatorTransport(self.path("gw.db"), clock=self.clock,
                                     fail_times=self.fail_times, reject=self.reject)
        self.o = InquiryOrchestrator(self.path("inquiry.db"), transport=self.gw,
                                     clock=self.clock)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def path(self, name: str) -> str:
        import os
        return os.path.join(self.dir, name)

    def new_node(self, *, auto_recover=False, owner=None, gw=None):
        """模拟另一个工作节点（同一文件库，独立连接）。"""
        return InquiryOrchestrator(self.path("inquiry.db"),
                                   transport=gw or self.gw, clock=self.clock,
                                   auto_recover=auto_recover, owner=owner)

    # -- 常用构造 ------------------------------------------------------------
    def seed_materials(self):
        self.o.register_material(
            material_id="MAT-CASE", version="1", category="CASE",
            content={"patient_name": "张三", "outcome": "恢复"},
            title="病例 v1", fact_key="OUTCOME", fact_value="恢复")
        self.o.register_material(
            material_id="MAT-REG", version="1", category="REGULATION",
            content={"article": "15日报告"}, title="法规 v1")
        self.o.register_material(
            material_id="MAT-STAT", version="1", category="STATISTIC",
            content={"prr": 2.1}, title="统计结论 v1")
        self.o.register_material(
            material_id="MAT-CASE2", version="1", category="CASE",
            content={"outcome": "未恢复"}, title="病例2",
            fact_key="OUTCOME", fact_value="未恢复")

    def register(self, *, questions=None, allowed=("MAT-CASE", "MAT-REG",
                                                   "MAT-STAT", "MAT-CASE2"),
                 jurisdiction="CN", deadline=DEADLINE):
        return self.o.register_inquiry(
            regulator="NMPA", jurisdiction=jurisdiction,
            questions=questions or [
                {"qid": "Q1", "text": "发生日期与转归？"},
                {"qid": "Q2", "text": "适用法规依据？"}],
            deadline=deadline, allowed_material_ids=list(allowed))

    def lock_base(self, refs=("CASE:MAT-CASE@1", "REG:MAT-REG@1")):
        inq = self.register()
        return inq, self.o.lock_draft(inq["id"], list(refs))

    def fill_and_seal(self, draft, *, attachments=True):
        """填好两个问题并定稿，返回 (submission_id, outgoing_number)。"""
        body1 = "2026-09-07 就诊，已恢复。证据 [CASE:MAT-CASE@1]"
        if attachments:
            a = self.o.add_attachment(
                draft["id"], filename="case.pdf", content=b"%PDF-case",
                derived_from_ref="CASE:MAT-CASE@1", uploaded_by="alice")
            body1 += f"。附件 @attachment:{a['id']}"
        self.o.submit_section(draft["id"], "Q1", body1, expected_revision=0,
                              submitted_by="alice")
        self.o.submit_section(draft["id"], "Q2",
                              "依据 [REG:MAT-REG@1]，15 日内报告。",
                              expected_revision=0, submitted_by="bob")
        sid = self.o.finalize(draft["id"], sealed_by="lead")["submission_id"]
        sub = self.o.submission_status(sid)
        return sid, sub["outgoing_number"]
