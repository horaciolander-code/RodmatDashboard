import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

interface ImportStatus {
  orders: number; products: number; combos: number; affiliates: number;
}

export default function Dashboard() {
  const { user } = useAuth();
  const [status, setStatus] = useState<ImportStatus>({ orders: 0, products: 0, combos: 0, affiliates: 0 });

  useEffect(() => {
    Promise.all([
      api.get('/sales/orders').then(r => r.data.length).catch(() => 0),
      api.get('/products').then(r => r.data.length).catch(() => 0),
      api.get('/combos').then(r => r.data.length).catch(() => 0),
      api.get('/sales/affiliates').then(r => r.data.length).catch(() => 0),
    ]).then(([orders, products, combos, affiliates]) => {
      setStatus({ orders, products, combos, affiliates });
    });
  }, []);

  const cards = [
    { label: 'Órdenes', count: status.orders, color: 'var(--rdmt-cyan)' },
    { label: 'Productos', count: status.products, color: 'var(--rdmt-green)' },
    { label: 'Combos', count: status.combos, color: 'var(--rdmt-purple)' },
    { label: 'Afiliados', count: status.affiliates, color: 'var(--rdmt-orange)' },
  ];

  const links = [
    { to: '/import', title: 'Importar Datos', desc: 'Sube CSV/Excel de tu tienda', icon: '📤' },
    { to: '/products', title: 'Productos', desc: 'Catálogo de productos', icon: '📦' },
    { to: '/combos', title: 'Combos', desc: 'Configura combos SKU', icon: '🎁' },
    { to: '/inventory', title: 'Inventario', desc: 'Stock inicial + pendiente', icon: '🏭' },
    { to: '/settings', title: 'Ajustes', desc: 'Configuración de tu tienda', icon: '⚙️' },
  ];

  return (
    <div>
      <h1 className="text-3xl rdmt-title mb-6">{user?.store_name ?? 'Rodmat'} · Panel</h1>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-10">
        {cards.map((c) => (
          <div key={c.label} className="rdmt-card p-5">
            <p className="text-3xl font-bold" style={{color: c.color, fontFamily: "'SF Mono', Menlo, monospace"}}>
              {c.count.toLocaleString()}
            </p>
            <p className="text-xs uppercase tracking-wider mt-2" style={{color: 'var(--rdmt-text-mut)'}}>{c.label}</p>
          </div>
        ))}
      </div>

      <h2 className="text-lg font-semibold mb-4 uppercase tracking-wider"
          style={{color: 'var(--rdmt-text-mut)'}}>Accesos Rápidos</h2>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {links.map(l => (
          <Link key={l.to} to={l.to} className="rdmt-card p-5 block no-underline">
            <div className="text-2xl mb-2">{l.icon}</div>
            <h3 className="font-semibold" style={{color: 'var(--rdmt-cyan)'}}>{l.title}</h3>
            <p className="text-sm mt-1" style={{color: 'var(--rdmt-text-mut)'}}>{l.desc}</p>
          </Link>
        ))}
      </div>
    </div>
  );
}
