# pesa_gui.py
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from executor import Database

class PesaDBGUI:
    def __init__(self, root):
        self.db = Database("data.pesa")
        self.tables = {}  # name -> Table object
        self.table_schemas = {}  # name -> [columns]

        root.title("PesaDB Browser — Your Simple DBeaver")
        root.geometry("1200x800")

        # Top toolbar
        toolbar = ttk.Frame(root)
        toolbar.pack(fill="x", padx=10, pady=5)
        ttk.Button(toolbar, text="➕ Create Table", command=self.create_table).pack(side="left", padx=5)
        ttk.Button(toolbar, text="🔀 Run Join", command=self.run_join_dialog).pack(side="left", padx=5)
        ttk.Button(toolbar, text="⟳ Refresh", command=self.refresh_tables).pack(side="left", padx=5)

        # Paned window: tables | data | join results
        main_paned = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
        main_paned.pack(expand=True, fill="both", padx=10, pady=5)

        # Left: Table list
        left_frame = ttk.LabelFrame(main_paned, text="Tables")
        self.table_list = tk.Listbox(left_frame, exportselection=False)
        self.table_list.pack(expand=True, fill="both", padx=5, pady=5)
        self.table_list.bind("<<ListboxSelect>>", self.on_table_select)
        main_paned.add(left_frame, weight=1)

        # Middle: Data grid
        middle_frame = ttk.LabelFrame(main_paned, text="Data")
        self.data_container = middle_frame
        main_paned.add(middle_frame, weight=2)

        # Right: Join results
        right_frame = ttk.LabelFrame(main_paned, text="Join Results")
        self.join_container = right_frame
        main_paned.add(right_frame, weight=2)

        self.refresh_tables()

    def refresh_tables(self):
        # For demo: assume common tables exist
        candidates = ["users", "orders", "products"]
        known = set(candidates)
        known.update(self.tables.keys())
        self.table_list.delete(0, tk.END)
        for name in sorted(known):
            self.table_list.insert(tk.END, name)

    def get_table(self, name):
        if name not in self.tables:
            try:
                self.tables[name] = self.db.get_table(name)
            except:
                return None
        return self.tables[name]

    def create_table(self):
        name = simpledialog.askstring("New Table", "Table name:")
        if not name: return
        cols = simpledialog.askstring("Columns", "Comma-separated column names (e.g., id,name,price):")
        if not cols: return
        columns = [c.strip() for c in cols.split(",") if c.strip()]
        if not columns:
            messagebox.showerror("Error", "Need at least one column")
            return

        try:
            tbl = self.db.create_table(name, columns)
            self.tables[name] = tbl
            self.table_schemas[name] = columns
            self.refresh_tables()
            messagebox.showinfo("Success", f"Created table '{name}'")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def on_table_select(self, event):
        if not self.table_list.curselection():
            return
        name = self.table_list.get(self.table_list.curselection())
        self.load_table_data(name)

    def load_table_data(self, table_name):
        # Clear previous UI
        for w in self.data_container.winfo_children():
            w.destroy()

        table = self.get_table(table_name)
        if not table:
            ttk.Label(self.data_container, text=f"Table '{table_name}' is empty.").pack(pady=20)
            return

        try:
            rows = table.select()
        except:
            rows = []

        if not rows and table_name in self.table_schemas:
            columns = self.table_schemas[table_name]
        elif rows:
            columns = list(rows[0].keys())
            self.table_schemas[table_name] = columns
        else:
            columns = ["id", "name"]  # fallback

        # Create treeview with editable cells
        tree = ttk.Treeview(self.data_container, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=100)
        tree.pack(expand=True, fill="both", side="top")

        # Insert data
        for row in rows:
            values = [row.get(col, "") for col in columns]
            tree.insert("", "end", values=values)

        # Control panel
        control = ttk.Frame(self.data_container)
        control.pack(fill="x", pady=5)

        # Entry fields for new row
        entries = {}
        for col in columns:
            ttk.Label(control, text=col).pack(side="left", padx=(5,0))
            var = tk.StringVar()
            entry = ttk.Entry(control, textvariable=var, width=10)
            entry.pack(side="left", padx=(0,5))
            entries[col] = var

        btn_frame = ttk.Frame(self.data_container)
        btn_frame.pack(fill="x", pady=5)
        ttk.Button(btn_frame, text="Insert", 
                  command=lambda: self.insert_row(table, entries, table_name)).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Update Selected", 
                  command=lambda: self.update_row(table, tree, columns, entries, table_name)).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Delete Selected", 
                  command=lambda: self.delete_row(table, tree, columns[0])).pack(side="left", padx=5)

        self.current_tree = tree
        self.current_columns = columns
        self.current_table_name = table_name

    def insert_row(self, table, entries, table_name):
        try:
            row = self._build_row_from_entries(entries)
            table.insert(row)
            self.load_table_data(table_name)
            messagebox.showinfo("Success", "Row inserted")
        except Exception as e:
            messagebox.showerror("Insert Error", str(e))

    def update_row(self, table, tree, columns, entries, table_name):
        sel = tree.selection()
        if not sel:
            messagebox.showwarning("Update", "Select a row to update")
            return
        try:
            # Get old key (assume first column is primary key)
            old_key = tree.item(sel[0])["values"][0]
            new_row = self._build_row_from_entries(entries)
            
            # Delete old, insert new (simple approach)
            table.delete(columns[0], old_key)
            table.insert(new_row)
            self.load_table_data(table_name)
            messagebox.showinfo("Success", "Row updated")
        except Exception as e:
            messagebox.showerror("Update Error", str(e))

    def delete_row(self, table, tree, key_col):
        sel = tree.selection()
        if not sel:
            messagebox.showwarning("Delete", "Select a row to delete")
            return
        try:
            key_val = tree.item(sel[0])["values"][0]
            table.delete(key_col, key_val)
            self.load_table_data(self.current_table_name)
            messagebox.showinfo("Success", "Row deleted")
        except Exception as e:
            messagebox.showerror("Delete Error", str(e))

    def _build_row_from_entries(self, entries):
        row = {}
        for col, var in entries.items():
            val = var.get()
            try:
                row[col] = int(val)
            except ValueError:
                try:
                    row[col] = float(val)
                except ValueError:
                    row[col] = val
        return row

    def run_join_dialog(self):
        # Clear join view
        for w in self.join_container.winfo_children():
            w.destroy()

        dialog = tk.Toplevel()
        dialog.title("Run Join")
        dialog.geometry("400x300")

        ttk.Label(dialog, text="Left Table:").pack(pady=5)
        left_var = tk.StringVar()
        left_combo = ttk.Combobox(dialog, textvariable=left_var, values=list(self.tables.keys()) or ["users", "orders"])
        left_combo.pack(pady=5)

        ttk.Label(dialog, text="Right Table:").pack(pady=5)
        right_var = tk.StringVar()
        right_combo = ttk.Combobox(dialog, textvariable=right_var, values=list(self.tables.keys()) or ["users", "orders"])
        right_combo.pack(pady=5)

        ttk.Label(dialog, text="Left Key Column:").pack(pady=5)
        left_key = tk.StringVar()
        left_key_entry = ttk.Entry(dialog, textvariable=left_key)
        left_key_entry.pack(pady=5)

        ttk.Label(dialog, text="Right Key Column:").pack(pady=5)
        right_key = tk.StringVar()
        right_key_entry = ttk.Entry(dialog, textvariable=right_key)
        right_key_entry.pack(pady=5)

        def execute_join():
            try:
                t1_name = left_var.get()
                t2_name = right_var.get()
                k1 = left_key.get()
                k2 = right_key.get()
                if not all([t1_name, t2_name, k1, k2]):
                    raise ValueError("All fields required")

                t1 = self.get_table(t1_name)
                t2 = self.get_table(t2_name)
                if not t1 or not t2:
                    raise ValueError("One or both tables not found")

                results = t1.hash_join(t2, k1, k2)
                self.display_join_results(results)
                dialog.destroy()
            except Exception as e:
                messagebox.showerror("Join Error", str(e))

        ttk.Button(dialog, text="Execute Join", command=execute_join).pack(pady=20)

    def display_join_results(self, results):
        for w in self.join_container.winfo_children():
            w.destroy()

        if not results:
            ttk.Label(self.join_container, text="No results").pack(pady=20)
            return

        columns = list(results[0].keys())
        tree = ttk.Treeview(self.join_container, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=100)
        tree.pack(expand=True, fill="both")

        for row in results:
            values = [row.get(col, "") for col in columns]
            tree.insert("", "end", values=values)

if __name__ == "__main__":
    root = tk.Tk()
    app = PesaDBGUI(root)
    root.mainloop()