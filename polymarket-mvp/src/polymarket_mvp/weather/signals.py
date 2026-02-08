from __future__ import annotations
from polymarket_mvp.weather.city_map import CITY_COORDS, infer_city
from polymarket_mvp.weather.sources import OpenMeteoSource, NwsSource


def blended_temp_c(city: str) -> tuple[float | None, dict]:
    lat, lon = CITY_COORDS[city]
    om = OpenMeteoSource().fetch_daily_max_c(lat, lon)
    nws = NwsSource().fetch_hourly_temp_c(lat, lon)

    vals = [v for v in [om, nws] if v is not None]
    blend = sum(vals) / len(vals) if vals else None
    return blend, {"open_meteo_max_c": om, "nws_hourly_c": nws}


def weather_market_hint(question: str, yes_hint: float, no_hint: float) -> dict:
    city = infer_city(question)
    if not city:
        return {"city": None, "blend_c": None, "note": "city_not_mapped"}

    blend, src = blended_temp_c(city)
    market_prob_yes = yes_hint if yes_hint > 0 else None
    market_prob_no = no_hint if no_hint > 0 else None

    # placeholder: for daily temp-range markets, use blended temp relative to rough pivot
    # until exact bucket parser is added.
    pivot_c = 20.0
    model_prob_yes = 0.6 if (blend is not None and blend >= pivot_c) else 0.4 if blend is not None else None

    divergence = None
    if model_prob_yes is not None and market_prob_yes is not None:
        divergence = model_prob_yes - market_prob_yes

    return {
        "city": city,
        "blend_c": blend,
        "market_prob_yes": market_prob_yes,
        "market_prob_no": market_prob_no,
        "model_prob_yes": model_prob_yes,
        "divergence": divergence,
        **src,
    }
