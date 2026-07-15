import React, { useState } from 'react';

export default function Optimize() {
  const [recommendations, setRecommendations] = useState([]);

  const fetchOptimize = async () => {
    try {
      const res = await fetch('http://localhost:8001/optimize');
      if (res.ok) {
        const data = await res.json();
        setRecommendations(data.recommendations);
      }
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div style={{ marginTop: '20px', padding: '15px', background: 'rgba(30, 41, 59, 0.7)', borderRadius: '1rem', border: '1px solid rgba(255, 255, 255, 0.1)' }}>
      <h2>Recommandations (IA)</h2>
      <button onClick={fetchOptimize} style={{ marginBottom: '15px' }}>Calculer l'allocation optimale</button>
      {recommendations.length > 0 ? (
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {recommendations.map((r, i) => (
            <li key={i} style={{ marginBottom: '10px', padding: '10px', background: 'rgba(0,0,0,0.2)', borderRadius: '0.5rem' }}>
              <div style={{ fontWeight: 'bold', marginBottom: '5px' }}>
                {r.type.toUpperCase()} - {r.zone_id} (Urgence: {r.urgency})
              </div>
              <div>{r.action}</div>
            </li>
          ))}
        </ul>
      ) : (
        <p style={{ color: 'var(--text-secondary)' }}>Aucune recommandation pour le moment.</p>
      )}
    </div>
  );
}
