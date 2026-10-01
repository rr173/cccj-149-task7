"""测试公共夹具与构造器。"""
import shutil
import tempfile
import unittest
from datetime import datetime

from pv_service.app import Application, FixedClock


class BasePVTest(unittest.TestCase):
    START = datetime(2026, 9, 10, 9, 0, 0)

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pv-test-")
        self.clock = FixedClock(self.START)
        self.app = Application(self.dir, clock=self.clock)
        self.app.seed_demo_reference()

    def tearDown(self):
        self.app.shutdown()
        shutil.rmtree(self.dir, ignore_errors=True)

    # -- 常用报文构造 --------------------------------------------------------
    @staticmethod
    def zhang_patient(**over):
        p = {"patient_name": "张三", "gender": "男", "dob": "1980-03-03",
             "patient_identifier": "ID-998",
             "drugs": [{"name": "布洛芬", "start_date": "2026-09-05"}],
             "reactions": ["皮疹"], "seriousness": "不严重",
             "event_date": "2026-09-07"}
        p.update(over)
        return p

    @staticmethod
    def zhang_multilang(**over):
        p = {"patient_name": "老张", "gender": "male", "dob": "1980-03-03",
             "patient_identifier": "ID-998",
             "drugs": [{"name": "ibuprofen", "start_date": "2026/09/05"}],
             "reactions": ["rash"], "seriousness": "mild",
             "event_date": "2026/09/07"}
        p.update(over)
        return p

    def pump(self):
        return self.app.reporting.pump()

    def db(self):
        import sqlite3
        from pv_service import db as dbmod
        conn = dbmod.connect(self.app.db_path)
        self.addCleanup(conn.close)
        return conn
