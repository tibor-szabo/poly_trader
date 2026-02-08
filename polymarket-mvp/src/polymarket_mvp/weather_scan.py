import re
from polymarket_mvp.config import load_config
from polymarket_mvp.adapters.gamma import GammaAdapter
from polymarket_mvp.weather.signals import weather_market_hint
from polymarket_mvp.utils.storage import append_event


def run(config_path: str = "config/default.yaml"):
    cfg = load_config(config_path)
    gamma = GammaAdapter(cfg["data"]["gamma_base"])

    refs = gamma.fetch_active_market_refs(limit=400)
    wx = re.compile(r"\b(temp|temperature|forecast|weather|rain|snow|hurricane|tornado|storm|climate|hottest|gust|precip)\b", re.I)
    sports_noise = re.compile(r"\b(nba|nhl|nfl|mlb|fifa|cup|stanley|heat|hurricanes?|bundesliga|goal scorer|finals?)\b", re.I)
    refs = [r for r in refs if wx.search(r.question) and not sports_noise.search(r.question)]

    out = []
    for r in refs[:30]:
        d = weather_market_hint(r.question, r.yes_price_hint, r.no_price_hint)
        out.append({
            "market_id": r.market_id,
            "question": r.question,
            **d,
        })

    append_event(cfg["storage"]["events_path"], {
        "type": "weather_scan",
        "count": len(out),
        "top": out[:10],
    })

    print(f"weather_markets={len(out)}")
    for x in out[:10]:
        print(x["market_id"], "|", x["question"][:70], "| city=", x.get("city"), "| div=", x.get("divergence"))


if __name__ == "__main__":
    run()
