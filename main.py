from fastapi import FastAPI, Request
from databases import Database
from database_interactions.insert_events import *

# Initialize database connection
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONNECTION_URL = os.getenv("DB_CONNECTION_URL")
database = Database(DB_CONNECTION_URL)

# Connect and disconnect to the database
async def lifespan(app: FastAPI):
    await database.connect()
    try:
        yield
    finally:
        # 
        await database.disconnect()

app = FastAPI(lifespan=lifespan)

@app.post("/product_viewed/")
async def product_viewed(request: Request):
    try:
        payload = await request.json()
        response = await insert_product_viewed(database, payload)
        return {"status": "success", "data": response}
    except Exception as e:
        return {"error": str(e)}

@app.post("/product_added_to_cart/")
async def product_added_to_cart(request: Request):
    try:
        payload = await request.json()
        response = await insert_product_added_to_cart(database, payload)
        return {"status": "success", "data": response}
    except Exception as e:
        return {"error": str(e)}

@app.post("/product_removed_from_cart/")
async def product_removed_from_cart(request: Request):
    try:
        payload = await request.json()
        response = await insert_product_viewed(database, payload)
        return {"status": "insert_product_removed_from_cart", "data": response}
    except Exception as e:
        return {"error": str(e)}

@app.post("/checkout_started/")
async def checkout_started(request: Request):
    try:
        payload = await request.json()
        response = await insert_checkout_started(database, payload)
        return {"status": "success", "data": response}
    except Exception as e:
        return {"error": str(e)}

@app.post("/checkout_completed/")
async def checkout_completed(request: Request):
    try:
        payload = await request.json()
        response = await insert_checkout_completed(database, payload)
        return {"status": "success", "data": response}
    except Exception as e:
        return {"error": str(e)}