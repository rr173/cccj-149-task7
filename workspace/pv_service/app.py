"""应用装配：把病例服务、报送服务、监管网关组装在一起。"""
from __future__ import annotations

from datetime import datetime, timedelta

from .countersign import ReviewService
from .export_service import ExportService
from .gateway import RegulatorGateway
from .reporting import ReportingService
from .rule_gov import RuleGovernanceService
from .rules import RuleEngine
from .service import CaseService, Clock
from .signal_service import SignalService


class FixedClock(Clock):
    """可推进的确定性时钟（测试/演示保证截止日期可复现）。"""

    def __init__(self, start: datetime):
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs):
        self._now += timedelta(**kwargs)

    def set(self, dt: datetime):
        self._now = dt


class Application:
    def __init__(self, data_dir: str = "data/pv",
                 clock: Clock | None = None,
                 gateway_fail_script: dict | None = None,
                 auto_dispatch: bool = False,
                 auto_export: bool = True,
                 poll_seconds: float = 1.0,
                 signer_seed: bytes | None = None,
                 export_crash_hook=None,
                 export_build_events=None,
                 countersign_required: bool = False,
                 allow_countersign_compat: bool = False,
                 review_lease_ttl_seconds: int = 900,
                 jurisdiction: str = "CN",
                 auto_rule_scheduler: bool = False,
                 serious_days: int = 15, listed_days: int = 30, routine_days: int = 90):
        self.clock = clock or Clock()
        self.db_path = f"{data_dir}/pv.db"
        self.gateway_db = f"{data_dir}/regulator_gateway.db"
        self.data_dir = data_dir
        self.fail_script = gateway_fail_script or {}
        engine = RuleEngine(serious_days=serious_days, listed_days=listed_days,
                            routine_days=routine_days)
        import threading
        self._lock = threading.RLock()
        self.countersign_required = countersign_required
        self.allow_countersign_compat = allow_countersign_compat
        self.cases = CaseService(self.db_path, rule_engine=engine, clock=self.clock,
                                 shared_lock=self._lock, jurisdiction=jurisdiction)
        # 双人会签服务（评审单 / 租约 / 签署 / 失效）。无论是否强制，
        # 评审接口都可用；countersign_required 只决定发送闸门是否强制。
        self.reviews = ReviewService(
            self.db_path, clock=self.clock, shared_lock=self._lock,
            lease_ttl_seconds=review_lease_ttl_seconds)
        self.reporting = ReportingService(
            self.db_path,
            gateway_fn=lambda: RegulatorGateway(self.gateway_db, clock=self.clock,
                                                fail_script=self.fail_script),
            clock=self.clock, auto_start=auto_dispatch, shared_lock=self._lock,
            poll_seconds=poll_seconds,
            countersign_required=countersign_required,
            countersign_gate=self.reviews.is_approved_for_dispatch,
            allow_countersign_compat=allow_countersign_compat)
        self.exports = ExportService(
            self.db_path, data_dir, clock=self.clock, signer_seed=signer_seed,
            auto_start=auto_export,
            poll_seconds=(0.05 if poll_seconds <= 1 else poll_seconds),
            crash_hook=export_crash_hook, build_events=export_build_events)
        # 药物安全法规规则治理层：登记/试算/受控启用/回滚/定时。
        # 解析器注入病例服务后，新报送计算固定到辖区当前生效规则版本。
        self.rules_gov = RuleGovernanceService(
            self.db_path, self.cases, clock=self.clock, shared_lock=self._lock,
            auto_start=auto_rule_scheduler,
            poll_seconds=(0.05 if poll_seconds <= 1 else poll_seconds))
        self.cases.rule_resolver = self.rules_gov.resolve
        # 药物安全信号窗口计算子系统（独立 signal.db；可复演运行/只增信号修订）。
        self.signals = SignalService(
            f"{data_dir}/signal.db", clock=self.clock, shared_lock=self._lock)

    def seed_demo_reference(self):
        # 已预期（listed）的药品-反应对
        self.cases.seed_listedness([
            ("ibuprofen", "rash"),
            ("ibuprofen", "nausea"),
            ("amoxicillin", "rash"),
            ("warfarin", "bleeding"),
        ])

    def shutdown(self):
        self.rules_gov.stop_scheduler()
        self.exports.stop_worker()
        self.reporting.stop_dispatcher()
