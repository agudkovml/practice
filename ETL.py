from datetime import datetime, timezone
import hashlib


def parse_ts(s: str) -> datetime:
    if 'z' in s.lower():
        s = (s.lower()).replace('z', '+00:00')
    dt = datetime.fromisoformat(s)
    dt = dt.astimezone(timezone.utc)
    return dt
    
def parse_money(s: str) -> float:
    return float(s.strip().replace(" ", "").replace(",", "."))

def user_hash(email: str, city: str, segment: str) -> str:
    separator = '|'
    info_user = separator.join([email, city, segment])
    info_user_h = hashlib.sha256(info_user.encode("utf-8")).hexdigest()
    return info_user_h

def make_storage() -> dict:
    return {
        "dwh": {
            "fct_orders": {},
            "dim_users_scd2": [],
            "dim_users_current_idx": {}
        },
        "etl_state": {
            "watermarks": {
                "orders": None,
                "users": None
            }
        }
    }
    
def upsert_order (storage: dict, raw_order: dict) -> tuple[int, int, int]:
    order_id = raw_order["order_id"] # Присвание уникального идентификатора
    
    parsed = { # Transform блок
        "order_id": order_id,
        "user_id": int(raw_order["user_id"]),
        "status": raw_order["status"],
        "total_amount": parse_money(raw_order["total_amount"]),
        "updated_at": parse_ts(raw_order["updated_at"])       
    }  
    
    fct_orders = storage["dwh"]["fct_orders"] # Ссылка на таблицу фактов
    
    if order_id not in fct_orders: # UPSERT-логика(принцип идемпотентности)
        fct_orders[order_id] = parsed
        return (1, 0, 0)
    
    existing = fct_orders[order_id] # Сравниваем версии по updated_at
    if parsed["updated_at"] > existing["updated_at"]:
        fct_orders[order_id] = parsed
        return (0, 1, 0)
    else:
        return (0, 0, 1)    
    
def process_orders_batch(storage: dict, raw_orders: list[dict]) -> dict:
    inserted = updated = skipped = 0 # Счётчики статистики
    
    for raw_order in raw_orders: # Проходим по каждому заказу и собираем статистику
        ins, upd, skp = upsert_order(storage, raw_order)
        inserted += ins
        updated += upd
        skipped += skp
        
    return {
        "processed": len(raw_orders),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped
    }
    
def scd2_upsert_user(storage: dict, raw_user: dict) -> tuple[int, int, int]:
    user_id = int(raw_user["user_id"])
    
    if user_id in storage["dwh"]["dim_users_scd2"]:   
