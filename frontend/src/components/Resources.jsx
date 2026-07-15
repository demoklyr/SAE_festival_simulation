import React, { useState, useEffect } from 'react';

export default function Resources() {
  const [resources, setResources] = useState([]);

  const fetchResources = async () => {
    try {
      const res = await fetch('http://localhost:8001/resources');
      if (res.ok) {
        const data = await res.json();
        setResources(data);
      }
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    fetchResources();
    const interval = setInterval(fetchResources, 5000);
    return () => clearInterval(interval);
  }, []);

  const restock = async (resource_id) => {
    try {
      await fetch(`http://localhost:8001/resources/${resource_id}/restock`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level_pct: 100 })
      });
      fetchResources();
    } catch (e) {
      console.error(e);
    }
  };

  const restockAll = async (type) => {
    try {
      await fetch(`http://localhost:8001/resources/restock?type=${type}&level_pct=100`, {
        method: 'POST'
      });
      fetchResources();
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div style={{ marginTop: '20px', padding: '15px', background: 'rgba(30, 41, 59, 0.7)', borderRadius: '1rem', border: '1px solid rgba(255, 255, 255, 0.1)' }}>
      <h2>Ressources & Logistique</h2>
      <div style={{ marginBottom: '10px' }}>
        <button onClick={() => restockAll('food')} style={{ marginRight: '10px' }}>Restock All Food</button>
        <button onClick={() => restockAll('water')}>Restock All Water</button>
      </div>
      <ul style={{ listStyle: 'none', padding: 0 }}>
        {resources.map(r => (
          <li key={r.resource_id} style={{ marginBottom: '10px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>
              <strong>{r.resource_id}</strong> ({r.type}) - {r.stock_level_pct !== null ? r.stock_level_pct.toFixed(1) : 0}%
            </span>
            <button onClick={() => restock(r.resource_id)}>Restock</button>
          </li>
        ))}
      </ul>
    </div>
  );
}
