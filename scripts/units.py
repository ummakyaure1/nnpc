"""
Unit conversion shared by the collectors. Targets: oil in bopd, gas in MMscf/d.
Figures are always converted in code (never by the AI), and every conversion is noted.
"""
import calendar
import re
from datetime import date

# Normalised unit -> (factor to target unit, is_daily_rate)
OIL_UNITS = {
    **{u: (1.0, True) for u in ["bopd", "bpd", "bbl/d", "bbls/d", "b/d", "bbl/day", "bbls/day",
                                "barrels/day", "barrels/d", "bbld", "bblsd", "stb/d", "bcpd"]},
    **{u: (1000.0, True) for u in ["kbopd", "kbpd", "kbbl/d", "kb/d", "kbcpd"]},
    **{u: (1.0, False) for u in ["bbl", "bbls", "barrels", "stb"]},
    **{u: (1e6, False) for u in ["mmbbl", "mmbbls", "mmstb"]},
}
GAS_UNITS = {
    **{u: (1.0, True) for u in ["mmscf/d", "mmscfd", "mmscf/day", "mmcf/d", "mmcfd", "mmscfpd"]},
    **{u: (1000.0, True) for u in ["bscf/d", "bcf/d", "bscfd", "bcfd"]},
    **{u: (1.0, False) for u in ["mmscf", "mmcf"]},
    **{u: (1000.0, False) for u in ["bscf", "bcf"]},
    **{u: (1e6, False) for u in ["tscf", "tcf"]},
    **{u: (0.001, False) for u in ["mscf", "mcf"]},
}
BOE_UNITS = {
    **{u: (1.0, True) for u in ["boepd", "boe/d", "boe/day"]},
    **{u: (1000.0, True) for u in ["kboepd", "kboe/d"]},
}
_MULTIPLIERS = [("thousand", 1e3), ("million", 1e6), ("000", 1e3)]


def normalise_unit(unit: str):
    u = re.sub(r"\(.*?\)", "", (unit or "").lower())
    u = re.sub(r"[\s.'’,]", "", u)
    multiplier = 1.0
    for word, factor in _MULTIPLIERS:
        if u.startswith(word):
            u, multiplier = u[len(word):], multiplier * factor
    u = u.replace("per", "/").replace("//", "/")
    u = re.sub(r"/(month|year|yr|annum|quarter|period)$", "", u)  # volume for the period
    return u, multiplier


def to_number(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else None
    try:
        n = float(str(value).replace(",", "").strip())
        return n if n >= 0 else None
    except ValueError:
        return None


def period_days(period: str | None = None, start: str | None = None, end: str | None = None):
    """Days covered by 'YYYY', 'YYYY-MM', or an explicit start/end (YYYY-MM-DD)."""
    try:
        if start and end:
            return (date.fromisoformat(end[:10]) - date.fromisoformat(start[:10])).days + 1
        if period and re.fullmatch(r"\d{4}", period):
            return 366 if calendar.isleap(int(period)) else 365
        if period and re.fullmatch(r"\d{4}-\d{2}", period):
            y, m = map(int, period.split("-"))
            return calendar.monthrange(y, m)[1]
    except ValueError:
        return None
    return None


def to_daily(value, unit, table: dict, days=None):
    """Returns (daily_rate or None, note). Volumes need the number of days in the period."""
    value = to_number(value)
    if value is None:
        return None, None
    key, multiplier = normalise_unit(unit)
    if key not in table:
        return None, f"unrecognised unit {unit!r}; left unconverted"
    factor, is_daily = table[key]
    factor *= multiplier
    notes = [f"x{factor:g} from {unit}"] if factor != 1.0 else []
    if is_daily:
        return round(value * factor, 3), "; ".join(notes) or None
    if not days:
        return None, f"volume in {unit} with no period length; left unconverted"
    return round(value * factor / days, 3), "; ".join(notes + [f"volume / {days} days"])
