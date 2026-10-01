"""启动 HTTP 服务：python -m pv_service.serve --data data/pv --port 8080"""
import argparse

from .app import Application
from .api import serve


def main():
    ap = argparse.ArgumentParser(description="药物不良事件病例接收与监管报送服务")
    ap.add_argument("--data", default="data/pv", help="数据目录")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--seed-demo", action="store_true", help="写入演示用 listedness 参考表")
    ap.add_argument("--require-countersign", action="store_true",
                    help="强制双人会签：未取得会签放行的待发送决定禁止派发")
    ap.add_argument("--allow-countersign-compat", action="store_true",
                    help="允许原有无需会签的调用在派发时显式带 allow_ungated=true 放行")
    ap.add_argument("--jurisdiction", default="CN",
                    help="本服务病例的默认法规辖区（规则版本作用域）")
    ap.add_argument("--auto-rule-scheduler", action="store_true",
                    help="启动规则治理后台调度：自动续跑中断任务并触发到点定时生效")
    args = ap.parse_args()

    app = Application(args.data, auto_dispatch=True,
                      countersign_required=args.require_countersign,
                      allow_countersign_compat=args.allow_countersign_compat,
                      jurisdiction=args.jurisdiction,
                      auto_rule_scheduler=args.auto_rule_scheduler)
    if args.seed_demo:
        app.seed_demo_reference()
    serve(app, args.host, args.port)


if __name__ == "__main__":
    main()
