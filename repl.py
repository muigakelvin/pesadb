# repl.py
from executor import Database

def main():
    print("PesaDB REPL v1.0")
    print("Commands: INS <table> <id> <val>, SEL <table>, DEL <table> <id>, JOIN t1 t2 ON k1 k2, exit")
    db = Database("data.pesa")  # ← your custom extension!

    # Auto-create demo tables
    try:
        users = db.get_table("users")
        orders = db.get_table("orders")
    except:
        users = db.create_table("users", ["id", "name"])
        orders = db.create_table("orders", ["order_id", "user_id"])

    while True:
        try:
            line = input("pesa> ").strip()
            if not line:
                continue

            parts = line.split()
            verb = parts[0].upper()

            if verb == "EXIT":
                break

            # ========== CREATE (Insert) ==========
            elif verb in ("INS", "INSERT"):
                if len(parts) < 4:
                    print("Usage: INS <table> <id> <value>")
                    continue
                table, id_str, value = parts[1], parts[2], parts[3]
                try:
                    id_val = int(id_str)
                    if table == "users":
                        users.insert({"id": id_val, "name": value})
                        print("✓ Inserted into users")
                    elif table == "orders":
                        user_id = int(value)
                        orders.insert({"order_id": id_val, "user_id": user_id})
                        print("✓ Inserted into orders")
                    else:
                        print("Unknown table. Use 'users' or 'orders'.")
                except ValueError:
                    print("Error: ID and user_id must be integers")

            # ========== READ (Select) ==========
            elif verb in ("SEL", "SELECT"):
                if len(parts) != 2:
                    print("Usage: SEL <table>")
                    continue
                table = parts[1]
                try:
                    if table == "users":
                        rows = users.select()
                    elif table == "orders":
                        rows = orders.select()
                    else:
                        print("Unknown table")
                        continue
                    if rows:
                        for r in rows:
                            print(r)
                    else:
                        print("(empty)")
                except Exception as e:
                    print("Select error:", e)

            # ========== DELETE ==========
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
                except ValueError:
                    print("Error: ID must be integer")
                except Exception as e:
                    print("Delete error:", e)

            # ========== JOIN ==========
            elif verb == "JOIN":
                if len(parts) == 6 and parts[3] == "ON":
                    t1, t2, k1, k2 = parts[1], parts[2], parts[4], parts[5]
                    try:
                        table1 = db.get_table(t1)
                        table2 = db.get_table(t2)
                        results = table1.hash_join(table2, k1, k2)
                        if results:
                            for r in results:
                                print(r)
                        else:
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