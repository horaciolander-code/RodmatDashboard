from pydantic import BaseModel, Field
from typing import List, Optional
from decimal import Decimal


class PLPlatformBlock(BaseModel):
    """Bloque P&L para una plataforma (TikTok, Amazon, o TOTAL)."""
    gross_subtotal:           float = 0  # Subtotal BEFORE discount
    seller_discount:          float = 0
    platform_discount:        float = 0
    gmv:                      float = 0  # = sku_subtotal_after_discount
    shipping_buyer:           float = 0  # cobrado al buyer
    platform_adjustment:      float = 0  # diferencia order_amount − GMV − shipping_buyer (a.k.a tax/ajustes)
    order_amount:             float = 0  # cobrado total al cliente
    refunds:                  float = 0
    net_order_amount:         float = 0  # order_amount − refunds
    # Costes
    cogs:                     float = 0  # combo decompose
    shipping_carrier:         float = 0  # original_shipping_fee (Smart Ship al seller)
    # Fees plataforma (auto-calculados)
    referral_fee:             float = 0  # 6% TikTok / 15% Amazon sobre GMV
    smart_promo_fee:          float = 0  # 3.5% TikTok sobre GMV
    smart_promo_campaign_fee: float = 0  # 1% TikTok sobre GMV
    fees_total:               float = 0  # suma fees plataforma
    # Otros
    creators_commission:      float = 0  # solo TOTAL (no por plataforma)


class CustomLineOut(BaseModel):
    id:          str
    description: str
    amount:      float
    sort_order:  float

    class Config:
        from_attributes = True


class CustomLineIn(BaseModel):
    description: str = Field(..., max_length=255)
    amount:      float
    sort_order:  Optional[float] = 0


class CustomLinesReplaceRequest(BaseModel):
    lines: List[CustomLineIn]


class PLResponse(BaseModel):
    """Respuesta del endpoint /api/finance/pl"""
    store_id:      str
    period_label:  str   # "Mayo 2026" o "YTD 2026"
    period_type:   str   # "month" | "ytd"
    year:          int
    month:         Optional[int] = None  # None si YTD
    period_start:  str   # ISO date
    period_end:    str   # ISO date (exclusivo)

    tiktok:        PLPlatformBlock
    amazon:        PLPlatformBlock
    total:         PLPlatformBlock

    # Resumen
    gross_margin:        float    # GMV/Net − COGS − shipping_neto − fees − creators − refunds
    shipping_net:        float    # shipping_carrier − shipping_buyer (lo que sale del bolsillo)

    custom_lines:        List[CustomLineOut]
    custom_total_income: float    # suma de amounts > 0
    custom_total_expense:float    # suma de amounts < 0 (valor absoluto)
    custom_net:          float    # suma neta

    net_result:          float    # gross_margin + custom_net
