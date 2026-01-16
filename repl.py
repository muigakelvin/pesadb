import sys
import os

# Add src/python to Python path so we can import executor
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "python"))

from executor import Database, Column, DataType

def main():
    print("PesaDB REPL v2.1 — Safe, persistent, with C hash join!")
    print("Commands: INS <table> ..., SEL <table> [WHERE col=val], DEL <table> <pk>, JOIN t1 t2 ON k1 k2, exit")
    
    # Use 'data/' subdirectory for database files
    db_path = os.path.join("data", "data.pesa")
    os.makedirs("data", exist_ok=True)  # Ensure data dir exists
    
    db = Database(db_path)

    if "users" not in db.tables:
        users = db.create_table("users", [
            Column("id", DataType.INT, primary_key=True),
            Column("name", DataType.TEXT, unique=True)
        ])
    else:
        users = db.get_table("users")

    if "orders" not in db.tables:
        orders = db.create_table("orders", [
            Column("order_id", DataType.INT, primary_key=True),
            Column("user_id", DataType.INT),
            Column("item", DataType.TEXT)
        ])
    else:
        orders = db.get_table("orders")

    while True:
        try:
            line = input("pesa> ").strip()
            if not line:
                continue

            parts = line.split()
            verb = parts[0].upper()

            if verb == "EXIT":
                break

            elif verb in ("INS", "INSERT"):
                if len(parts) < 4:
                    print("Usage: INS <table> <id> <value> [item]")
                    continue
                table = parts[1]
                try:
                    if table == "users":
                        id_val = int(parts[2])
                        name = parts[3]
                        users.insert({"id": id_val, "name": name})
                        print("✓ Inserted into users")
                    elif table == "orders":
                        order_id = int(parts[2])
                        user_id = int(parts[3])
                        item = parts[4] if len(parts) > 4 else "default"
                        orders.insert({"order_id": order_id, "user_id": user_id, "item": item})
                        print("✓ Inserted into orders")
                    else:
                        print("Unknown table")
                except Exception as e:
                    print("Insert error:", e)

            elif verb in ("SEL", "SELECT"):
                if len(parts) < 2:
                    print("Usage: SEL <table> [WHERE col=value]")
                    continue
                table = parts[1]
                where_col, where_val = None, None
                if len(parts) >= 5 and parts[2].upper() == "WHERE":
                    where_col = parts[3]
                    where_val_str = parts[4]
                    try:
                        where_val = int(where_val_str)
                    except ValueError:
                        where_val = where_val_str

                try:
                    if table == "users":
                        rows = users.select(where_col, where_val)
                    elif table == "orders":
                        rows = orders.select(where_col, where_val)
                    else:
                        print("Unknown table")
                        continue
                    for r in rows:
                        print(r)
                    if not rows:
                        print("(empty)")
                except Exception as e:
                    print("Select error:", e)

            elif verb in ("DEL", "DELETE"):
                if len(parts) != 3:
                    print("Usage: DEL <table> <id>")
                    continue
                table, id_str = parts[1], parts[2]
                try:
                    id_val = int(id_str)
                    if table == "users":
                        users.delete("id", id_val)
                        print("✓ Deleted from users")
                    elif table == "orders":
                        orders.delete("order_id", id_val)
                        print("✓ Deleted from orders")
                    else:
                        print("Unknown table")
                except Exception as e:
                    print("Delete error:", e)

            elif verb == "JOIN":
                if len(parts) == 6 and parts[3] == "ON":
                    t1, t2, k1, k2 = parts[1], parts[2], parts[4], parts[5]
                    try:
                        table1 = db.get_table(t1)
                        table2 = db.get_table(t2)
                        results = table1.hash_join(table2, k1, k2)
                        for r in results:
                            print(r)
                        if not results:
                            print("(no matches)")
                    except Exception as e:
                        print("Join error:", e)
                else:
                    print("Usage: JOIN <t1> <t2> ON <key1> <key2>")

            else:
                print(f"Unknown command: '{parts[0]}'. Try: INS, SEL, DEL, JOIN, exit")

        except KeyboardInterrupt:
            print("\nBye!")
            break
        except EOFError:
            break
        except Exception as e:
            print("Unexpected error:", e)

    print("Goodbye from PesaDB!")

if __name__ == "__main__":
    main()