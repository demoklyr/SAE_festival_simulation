import React from 'react';
import { useWebSocket } from './hooks/useWebSocket';
import ZoneCard from './components/ZoneCard';
import LiveAlerts from './components/LiveAlerts';
import './index.css';


const WS_URL = 'ws://localhost:8001/ws/live';

function App() {
  const { data, isConnected, error } = useWebSocket(WS_URL);

  return (
    <div className="app-container">
      {/* Header & Connection Status */}
      <header className="app-header">
        <h1>FestivalOS Dashboard</h1>
        <div className="status-indicators">
          {error ? (
            <div className="connection-error">
              ⚠️ {error}
            </div>
          ) : (
            <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
              <span className="dot"></span>
              {isConnected ? 'En direct' : 'Connexion...'}
            </div>
          )}
        </div>
      </header>

      {/* Main Content Area */}
      <main className="dashboard-main">
        {/* Zones Grid */}
        <div className="zones-section">
          <h2>Vue d'ensemble des scènes</h2>
          <div className="zones-grid">
            {data.zones && data.zones.length > 0 ? (
              data.zones.map(zone => (
                <ZoneCard key={zone.zone_id} zone={zone} />
              ))
            ) : (
              <p className="loading-text">En attente de données...</p>
            )}
          </div>
        </div>

        {/* Alerts Sidebar */}
        <aside className="alerts-section">
          <LiveAlerts alerts={data.alerts} />
        </aside>
      </main>
    </div>
  );
}

export default App;
