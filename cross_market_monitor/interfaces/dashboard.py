from cross_market_monitor.interfaces.api.app import create_app, render_dashboard_html


def _dashboard_html(title: str) -> str:
    return render_dashboard_html(title)
