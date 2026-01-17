# api.py
import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

# Use absolute path to ensure consistent DB location
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "data.pesa")

# Add src/python to Python path
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src", "python"))

from executor import Database, Column, DataType

# Initialize DB with absolute path
db = Database(DB_PATH)

# Create tables if missing
if "users" not in db.tables:
    db.create_table("users", [
        Column("id", DataType.INT, primary_key=True),
        Column("name", DataType.TEXT, unique=True)
    ])

if "orders" not in db.tables:
    db.create_table("orders", [
        Column("order_id", DataType.INT, primary_key=True),
        Column("user_id", DataType.INT),
        Column("item", DataType.TEXT)
    ])

app = FastAPI(title="PesaDB Web API")

# Models WITHOUT id fields
class UserCreate(BaseModel):
    name: str  # ← no id

class OrderCreate(BaseModel):
    user_id: int
    item: str  # ← no order_id

class UserResponse(BaseModel):
    id: int
    name: str

class OrderResponse(BaseModel):
    order_id: int
    user_id: int
    item: str

def get_next_user_id():
    """Get next available user ID (max + 1)"""
    users = db.get_table("users")
    rows = users.select()
    if not rows:
        return 1
    return max(row["id"] for row in rows) + 1

def get_next_order_id():
    """Get next available order ID (max + 1)"""
    orders = db.get_table("orders")
    rows = orders.select()
    if not rows:
        return 1
    return max(row["order_id"] for row in rows) + 1

@app.post("/users/", response_model=UserResponse)
def create_user(user: UserCreate):
    try:
        user_id = get_next_user_id()
        users = db.get_table("users")
        users.insert({"id": user_id, "name": user.name})
        return {"id": user_id, "name": user.name}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"[ERROR] User creation failed: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/users/", response_model=List[UserResponse])
def get_users():
    try:
        users = db.get_table("users")
        return users.select()
    except Exception as e:
        print(f"[ERROR] Fetch users failed: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

@app.post("/orders/", response_model=OrderResponse)
def create_order(order: OrderCreate):
    try:
        # Validate user exists
        users = db.get_table("users")
        if not users.select(where_col="id", where_val=order.user_id):
            raise HTTPException(status_code=400, detail="User not found")

        order_id = get_next_order_id()
        orders = db.get_table("orders")
        orders.insert({
            "order_id": order_id,
            "user_id": order.user_id,
            "item": order.item
        })
        return {"order_id": order_id, "user_id": order.user_id, "item": order.item}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Order creation failed: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/users/{user_id}/orders", response_model=List[OrderResponse])
def get_user_orders(user_id: int):
    try:
        orders = db.get_table("orders")
        return orders.select(where_col="user_id", where_val=user_id)
    except Exception as e:
        print(f"[ERROR] Fetch orders failed: {e}")
        raise HTTPException(status_code=500, detail="Internal error")