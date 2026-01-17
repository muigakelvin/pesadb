// frontend/src/App.js
import React, { useState, useEffect } from "react";

function App() {
  const [users, setUsers] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState(null);
  const [userOrders, setUserOrders] = useState([]);
  const [newUserName, setNewUserName] = useState("");
  const [newOrderItem, setNewOrderItem] = useState("");

  useEffect(() => {
    fetchUsers();
  }, []);

  const fetchUsers = async () => {
    const res = await fetch("/users/");
    const data = await res.json();
    setUsers(data);
  };

  const createUser = async (e) => {
    e.preventDefault();
    if (!newUserName.trim()) return;
    await fetch("/users/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: newUserName }),
    });
    setNewUserName("");
    fetchUsers();
  };

  const createOrder = async (e) => {
    e.preventDefault();
    if (!newOrderItem.trim() || !selectedUserId) return;
    await fetch("/orders/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: selectedUserId,
        item: newOrderItem,
      }),
    });
    setNewOrderItem("");
    fetchUserOrders(selectedUserId);
  };

  const fetchUserOrders = async (userId) => {
    const res = await fetch(`/users/${userId}/orders`);
    const data = await res.json();
    setUserOrders(data);
    setSelectedUserId(userId);
  };

  return (
    <div
      style={{ padding: "20px", fontFamily: "sans-serif", maxWidth: "700px" }}
    >
      <h1>🛒 PesaDB Web Interface</h1>

      <section>
        <h2>👤 Register User</h2>
        <form onSubmit={createUser}>
          <input
            type="text"
            placeholder="Name"
            value={newUserName}
            onChange={(e) => setNewUserName(e.target.value)}
            required
            style={{ margin: "4px", padding: "6px", width: "200px" }}
          />
          <button type="submit" style={{ margin: "4px", padding: "6px" }}>
            Add User
          </button>
        </form>
      </section>

      <section>
        <h2>👥 Users</h2>
        <ul>
          {users.map((user) => (
            <li key={user.id} style={{ margin: "8px 0" }}>
              <strong>{user.id}</strong>: {user.name}
              <button
                onClick={() => fetchUserOrders(user.id)}
                style={{ marginLeft: "12px", padding: "4px 8px" }}
              >
                View Orders
              </button>
            </li>
          ))}
        </ul>
      </section>

      {selectedUserId && (
        <>
          <section>
            <h2>📦 Add Order for User {selectedUserId}</h2>
            <form onSubmit={createOrder}>
              <input
                type="text"
                placeholder="Item"
                value={newOrderItem}
                onChange={(e) => setNewOrderItem(e.target.value)}
                required
                style={{ margin: "4px", padding: "6px", width: "200px" }}
              />
              <button type="submit" style={{ margin: "4px", padding: "6px" }}>
                Add Order
              </button>
            </form>
          </section>

          <section>
            <h2>📋 Orders</h2>
            {userOrders.length === 0 ? (
              <p>No orders yet.</p>
            ) : (
              <ul>
                {userOrders.map((order) => (
                  <li key={order.order_id}>
                    Order #{order.order_id}: "{order.item}"
                  </li>
                ))}
              </ul>
            )}
          </section>
        </>
      )}
    </div>
  );
}

export default App;
