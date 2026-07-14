import { useState, useEffect, useRef } from 'react';

export function useWebSocket(url) {
  const [data, setData] = useState({ zones: [], alerts: [] });
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState(null);
  const ws = useRef(null);

  useEffect(() => {
    let reconnectTimeout;

    function connect() {
      ws.current = new WebSocket(url);

      ws.current.onopen = () => {
        setIsConnected(true);
        setError(null);
        console.log('Connecté au WebSocket');
      };

      ws.current.onmessage = (event) => {
        try {
          const parsedData = JSON.parse(event.data);
          if (parsedData.type === 'update') {
            setData({
              zones: parsedData.zones || [],
              alerts: parsedData.alerts || []
            });
          }
        } catch (e) {
          console.error("Erreur de parsing WS", e);
        }
      };

      ws.current.onclose = () => {
        setIsConnected(false);
        setError('Connexion au serveur perdue');
        console.log('Déconnecté du WebSocket. Reconnexion dans 3s...');
        // Tentative de reconnexion
        reconnectTimeout = setTimeout(connect, 3000);
      };

      ws.current.onerror = (err) => {
        console.error('Erreur WebSocket:', err);
        ws.current.close();
      };
    }

    connect();

    return () => {
      if (ws.current) {
        ws.current.close();
      }
      clearTimeout(reconnectTimeout);
    };
  }, [url]);

  return { data, isConnected, error };
}
