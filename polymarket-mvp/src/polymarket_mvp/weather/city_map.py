CITY_COORDS = {
    "new york": (40.7128, -74.0060),
    "nyc": (40.7128, -74.0060),
    "toronto": (43.6532, -79.3832),
    "chicago": (41.8781, -87.6298),
    "atlanta": (33.7490, -84.3880),
    "dallas": (32.7767, -96.7970),
    "miami": (25.7617, -80.1918),
    "seattle": (47.6062, -122.3321),
    "london": (51.5072, -0.1276),
    "seoul": (37.5665, 126.9780),
    "ankara": (39.9334, 32.8597),
    "wellington": (-41.2866, 174.7756),
    "buenos aires": (-34.6037, -58.3816),
}


from typing import Optional


def infer_city(question: str) -> Optional[str]:
    q = (question or "").lower()
    for city in CITY_COORDS:
        if city in q:
            return city
    return None
