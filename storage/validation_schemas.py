from datetime import date, datetime

from pydantic import BaseModel, Field


class OptionChainRowIn(BaseModel):
    fetched_at: datetime
    source_account: str | None = None
    expiry: date
    strike: float = Field(gt=0)
    option_type: str = Field(pattern="^(CE|PE)$")
    ltp: float | None = Field(default=None, ge=0)
    oi: int | None = Field(default=None, ge=0)
    prev_oi: int | None = Field(default=None, ge=0)
    volume: int | None = Field(default=None, ge=0)
    iv: float | None = Field(default=None, ge=0)
    delta: float | None = None
    theta: float | None = None
    gamma: float | None = None
    vega: float | None = None
    bid: float | None = Field(default=None, ge=0)
    ask: float | None = Field(default=None, ge=0)
    underlying_ltp: float | None = Field(default=None, ge=0)
    security_id: str | None = None

    def dedupe_key(self) -> str:
        return f"optionchain:{self.expiry}:{self.strike}:{self.option_type}:{self.fetched_at.isoformat()}"


class TickDataIn(BaseModel):
    fetched_at: datetime
    source_account: str | None = None
    security_id: str
    symbol: str
    ltp: float | None = Field(default=None, ge=0)
    ltt: datetime | None = None
    volume: int | None = Field(default=None, ge=0)
    oi: int | None = Field(default=None, ge=0)
    bid_depth: list | dict | None = None
    ask_depth: list | dict | None = None

    def dedupe_key(self) -> str:
        return f"tick:{self.security_id}:{self.source_account}:{self.fetched_at.isoformat()}"
