import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import api from '../services/api';

interface ImportStatus {
  orders: number;
  products: number;
  combos: number;
  affiliates: number;
}

export default function Dashboard() {
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
    { label: 'Orders', count: status.orders, color: 'bg-blue-500' },
    { label: 'Products', count: status.products, color: 'bg-green-500' },
    { label: 'Combos', count: status.combos, color: 'bg-purple-500' },
    { label: 'Affiliates', count: status.affiliates, color: 'bg-orange-500' },
  ];

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Dashboard</h1>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        {cards.map((c) => (
          <div key={c.label} className={`${c.color} text-white rounded-lg p-6`}>
            <p className="text-3xl font-bold">{c.count.toLocaleString()}</p>
            <p className="text-sm opacity-80">{c.label}</p>
          </div>
        ))}
      </div>

      <h2 className="text-xl font-semibold mb-4">Quick Actions</h2>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Link to="/import" className="block p-4 bg-white border rounded-lg hover:shadow-md transition">
          <h3 className="font-semibold">Import Data</h3>
          <p className="text-sm text-gray-500">Upload CSV/Excel files</p>
        </Link>
        <Link to="/products" className="block p-4 bg-white border rounded-lg hover:shadow-md transition">
          <h3 className="font-semibold">Products</h3>
          <p className="text-sm text-gray-500">Manage product catalog</p>
        </Link>
        <Link to="/combos" className="block p-4 bg-white border rounded-lg hover:shadow-md transition">
          <h3 className="font-semibold">Combos</h3>
          <p className="text-sm text-gray-500">Configure combo SKUs</p>
        </Link>
        <Link to="/inventory" className="block p-4 bg-white border rounded-lg hover:shadow-md transition">
          <h3 className="font-semibold">Inventory</h3>
          <p className="text-sm text-gray-500">Initial + incoming stock</p>
        </Link>
        <Link to="/settings" className="block p-4 bg-white border rounded-lg hover:shadow-md transition">
          <h3 className="font-semibold">Settings</h3>
          <p className="text-sm text-gray-500">Store configuration</p>
        </Link>
      </div>
    </div>
  );
}
