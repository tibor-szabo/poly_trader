import httpx
from typing import Optional


class OpenMeteoSource:
    BASE = "https://api.open-meteo.com/v1/forecast"

    def fetch_daily_max_c(self, lat: float, lon: float) -> Optional[float]:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max",
            "timezone": "UTC",
            "forecast_days": 1,
        }
        with httpx.Client(timeout=15.0) as c:
            r = c.get(self.BASE, params=params)
            if r.status_code != 200:
                return None
            j = r.json()
        vals = (j.get("daily") or {}).get("temperature_2m_max") or []
        return float(vals[0]) if vals else None


class NwsSource:
    BASE = "https://api.weather.gov"

    def fetch_hourly_temp_c(self, lat: float, lon: float) -> Optional[float]:
        with httpx.Client(timeout=15.0, headers={"User-Agent": "JarvisMVP/1.0"}) as c:
            p = c.get(f"{self.BASE}/points/{lat},{lon}")
            if p.status_code != 200:
                return None
            hourly = ((p.json().get("properties") or {}).get("forecastHourly"))
            if not hourly:
                return None
            h = c.get(hourly)
            if h.status_code != 200:
                return None
            periods = ((h.json().get("properties") or {}).get("periods") or [])
        if not periods:
            return None
        t_f = periods[0].get("temperature")
        if t_f is None:
            return None
        return (float(t_f) - 32.0) * 5.0 / 9.0
