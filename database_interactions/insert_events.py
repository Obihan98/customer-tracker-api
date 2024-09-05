from datetime import datetime
import json

async def insert_product_viewed(database, payload):
    query = """
        INSERT INTO t_product_viewed (clientId, eventId, productId, productTitle, productUrl, productImage, productPrice, currency, timestamp)
        VALUES (:client_id, :event_id, :product_id, :product_title, :product_url, :product_image, :product_price, :currency, :timestamp);
    """
    values = {
        "client_id": payload['clientId'],
        "event_id": payload['eventId'],
        "product_id": payload['product_id'],
        "product_title": payload['product_title'],
        "product_url": payload['product_url'],
        "product_image": payload['product_image'],
        "product_price": str(payload['product_price']),
        "currency": payload['currency'],
        "timestamp": datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
    }
    try:
        result = await database.execute(query, values)
        return result
    except Exception as e:
        print("DATABASE ERROR: Could not insert 'product_viewed' into t_product_viewed", str(e))

async def insert_product_added_to_cart(database, payload):
    query = """
        INSERT INTO t_product_added_to_cart (clientId, eventId, productId, productTitle, productUrl, productImage, productPrice, productQuantity, currency, timestamp)
        VALUES (:client_id, :event_id, :product_id, :product_title, :product_url, :product_image, :product_price, :product_quantity, :currency, :timestamp);
    """
    values = {
        "client_id": payload['clientId'],
        "event_id": payload['eventId'],
        "product_id": payload['product_id'],
        "product_title": payload['product_title'],
        "product_url": payload['product_url'],
        "product_image": payload['product_image'],
        "product_price": str(payload['product_price']),
        "product_quantity": str(payload['product_quantity']),
        "currency": payload['currency'],
        "timestamp": datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
    }
    try:
        result = await database.execute(query, values)
        return result
    except Exception as e:
        print("DATABASE ERROR: Could not insert 't_product_added_to_cart'", str(e))
        
async def insert_product_removed_from_cart(database, payload):
    query = """
        INSERT INTO t_product_remove_from_cart (clientId, eventId, productId, productImage, productPrice, productQuantity, currency, timestamp)
        VALUES (:client_id, :event_id, :product_id, :product_image, :product_price, :product_quantity, :currency, :timestamp);
    """
    values = {
        "client_id": payload['clientId'],
        "event_id": payload['eventId'],
        "product_id": payload['product_id'],
        "product_image": payload['product_image'],
        "product_price": str(payload['product_price']),
        "product_quantity": str(payload['product_quantity']),
        "currency": payload['currency'],
        "timestamp": datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
    }
    try:
        result = await database.execute(query, values)
        return result
    except Exception as e:
        print("DATABASE ERROR: Could not insert 't_product_remove_from_cart'", str(e))
     
async def insert_checkout_started(database, payload):
    query = """
        INSERT INTO t_checkout_started (clientId, eventId, checkoutTotal, checkoutItems, currency, timestamp)
        VALUES (:client_id, :event_id, :checkout_total, :checkout_items, :currency, :timestamp);
    """
    values = {
        "client_id": payload['clientId'],
        "event_id": payload['eventId'],
        "checkout_total": str(payload['checkout_total']),
        "checkout_items": [json.dumps(item) for item in payload['checkout_items']],
        "currency": payload['currency'],
        "timestamp": datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
    }
    try:
        result = await database.execute(query, values)
        return result
    except Exception as e:
        print("DATABASE ERROR: Could not insert into 't_checkout_started'", str(e))
        
async def insert_checkout_completed(database, payload):
    query = """
        INSERT INTO t_checkout_completed (clientId, eventId, checkoutTotal, checkoutItems, currency, timestamp)
        VALUES (:client_id, :event_id, :checkout_total, :checkout_items, :currency, :timestamp);
    """
    values = {
        "client_id": payload['clientId'],
        "event_id": payload['eventId'],
        "checkout_total": str(payload['checkout_total']),
        "checkout_items": [json.dumps(item) for item in payload['checkout_items']],
        "currency": payload['currency'],
        "timestamp": datetime.fromisoformat(payload['timestamp'].replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
    }
    try:
        result = await database.execute(query, values)
        return result
    except Exception as e:
        print("DATABASE ERROR: Could not insert into 't_checkout_completed'", str(e))