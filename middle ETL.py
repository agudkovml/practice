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
    dim = storage["dwh"]["dim_users_scd2"] # Достаём таблицу и индекс "текущих" записей
    cur_idx = storage["dwh"]["dim_users_current_idx"]
    
    user_id = int(raw_user["user_id"]) # TRANSFORM: приводим сырые строки CSV к нужным типам
    email = raw_user["email"]
    city = raw_user["city"]
    segment = raw_user["segment"]
    updated_at = parse_ts(raw_user["updated_at"])  
    
    h = user_hash(email, city, segment) # Вычисляем хэш
    
    if user_id not in cur_idx:  # Пользователя ещё нет -> вставляем первую (и текущую) версию
        new_row = {
            "user_id": user_id,
            "email": email,
            "city": city,
            "segment": segment,
            "updated_at": updated_at,
            "valid_from": updated_at,
            "valid_to": None, 
            "is_current": True,
            "hash_diff": h,
        }
        dim.append(new_row)
        cur_idx[user_id] = len(dim) - 1
        return (1, 0, 0) 
    
    i = cur_idx[user_id] # Пользователь существует
    current = dim[i]
    
    if h == current["hash_diff"]:  # Если атрибуты не изменились (hash тот же) -> ничего не делаем

        return (0, 0, 1)

    current["valid_to"] = updated_at # Атрибуты изменились -> закрываем старую версию (SCD2)
    current["is_current"] = False

    new_row = {  # Вставляем новую текущую версию
        "user_id": user_id,
        "email": email,
        "city": city,
        "segment": segment,
        "updated_at": updated_at,
        "valid_from": updated_at,
        "valid_to": None,
        "is_current": True,
        "hash_diff": h,
    }
    dim.append(new_row)
    cur_idx[user_id] = len(dim) - 1

    return (1, 1, 0)

def process_users_batch(storage: dict, raw_users: list[dict]) -> dict:
    inserted_new = closed_old = skipped = 0 # Счётчики статистики
    
    for raw_user in raw_users: # Проходим по каждому заказу и собираем статистику
        ins, clo, skp = upsert_order(storage, raw_users)
        inserted_new += ins
        closed_old += clo
        skipped += skp
        
    return {
        "processed": len(raw_users),
        "inserted_new": inserted_new,
        "closed_old": closed_old,
        "skipped": skipped
    }
    
def update_watermarks(storage: dict, entity: str, max_updated_at: datetime): #Сохраняем максимальный updated_at после батча для инкрементальности
    storage["etl_state"]["watermarks"][entity] = max_updated_at
    
def run_etl_batch(storage, batch_date: str, raw_orders: list[dict], raw_users: list[dict]) -> dict:    
    # Загружаем факты (заказы)
    orders_stats = process_orders_batch(storage, raw_orders)
    
    # Загружаем измерения (пользователи SCD2)
    users_stats = process_users_batch(storage, raw_users)
    
    #  Watermark
    if raw_orders:
        max_order_ts = max(parse_ts(o["updated_at"]) for o in raw_orders)
        update_watermarks(storage, "orders", max_order_ts)
    
    if raw_users:
        max_user_ts = max(parse_ts(u["updated_at"]) for u in raw_users)
        update_watermarks(storage, "users", max_user_ts)
    
    # Общая статистика
    return {
        "batch_date": batch_date,
        "orders": orders_stats,
        "users": users_stats,
        "watermarks": storage["etl_state"]["watermarks"]
    }