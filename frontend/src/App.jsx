import React, { useState } from 'react';
import { useWebSocket } from './hooks/useWebSocket';
import FestivalMap from './components/FestivalMap';
import LiveAlerts from './components/LiveAlerts';
import Resources from './components/Resources';
import Optimize from './components/Optimize';
import { Radio, AlertCircle, Settings, ChevronDown } from 'lucide-react';
import './index.css';

const WS_URL = 'ws://localhost:8001/ws/live';

function App() {
  const { data, isConnected, error } = useWebSocket(WS_URL);
  const [selectedZoneId, setSelectedZoneId] = useState(null);
  const [toolsOpen, setToolsOpen] = useState(false);

  return (
    <div className="app-container">
      {/* Header & Connection Status */}
      <header className="app-header">
        <h1>FestivalOS</h1>
        <div className="header-right">
          {/* Menu Outils */}
          <div className="tools-dropdown-wrapper">
            <button className="tools-btn" onClick={() => setToolsOpen(!toolsOpen)}>
              <Settings size={16} /> Outils <ChevronDown size={14} className={toolsOpen ? 'chevron-open' : ''} />
            </button>
            {toolsOpen && (
              <div className="tools-dropdown" onClick={(e) => e.stopPropagation()}>
                <Optimize />
                <Resources />
              </div>
            )}
          </div>

          {/* Status */}
          {error ? (
            <div className="connection-error">
              <AlertCircle size={16} /> {error}
            </div>
          ) : (
            <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
              <span className="dot"></span>
              {isConnected ? (
                <>
                  <Radio size={14} color="var(--success)" className="critical-pulse" style={{ animationDuration: '2s', border: 'none' }} /> 
                  Live
                </>
              ) : 'Connecting...'}
            </div>
          )}
        </div>
      </header>

      {/* Main Content Area */}
      <main className="dashboard-main">
        {/* Map Section */}
        <div className="zones-section">
          <h2>Carte Intuitive du Festival</h2>
          <FestivalMap 
            zones={data.zones} 
            selectedZoneId={selectedZoneId}
            onSelectZone={setSelectedZoneId}
          />
        </div>

        {/* Sidebar - Alertes uniquement */}
        <aside className="alerts-section">
          <LiveAlerts alerts={data.alerts} />
        </aside>
      </main>
    </div>
  );
}

export default App;
