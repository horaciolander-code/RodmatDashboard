import { useEffect, useState } from 'react';
import api from '../services/api';

interface InvRecord {
  id: string;
  product_id: string;
  product_name?: string;
  quantity?: number;
  qty_ordered?: number;
  order_date?: string;
  start_date?: string;
  status?: string;
  supplier?: string;
  tracking?: string;
  cost?: number;
  notes?: string;
}

const STATUS_COLORS: Record<string, string> = {
  'Recibido':          'bg-green-100 text-green-700',
  'recibido':          'bg-green-100 text-green-700',
  'pending':           'bg-yellow-100 text-yellow-700',
  'Pending':           'bg-yellow-100 text-yellow-700',
  'En tránsito':       'bg-blue-100 text-blue-700',
  'Cancelado':         'bg-red-100 text-red-700',
};

function statusBadge(s?: string) {
  const cls = (s && STATUS_COLORS[s]) || 'bg-gray-100 text-gray-600';
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{s || '-'}</span>;
}

export default function Inventory() {
  const [tab, setTab] = useState<'initial' | 'incoming'>('initial');
  const [initial, setInitial] = useState<InvRecord[]>([]);
  const [incoming, setIncoming] = useState<InvRecord[]>([]);
  const [search, setSearch] = useState('');

  const load = () => {
    api.get('/inventory/initial').then(r => setInitial(r.data)).catch(() => {});
    api.get('/inventory/incoming').then(r => setIncoming(r.data)).catch(() => {});
  };
  useEffect(() => { load(); }, []);

  const filterRows = (rows: InvRecord[]) => {
    if (!search.trim()) return rows;
    const q = search.toLowerCase();
    return rows.filter(r =>
      (r.product_name || r.product_id).toLowerCase().includes(q) ||
      (r.supplier || '').toLowerCase().includes(q) ||
      (r.tracking || '').toLowerCase().includes(q)
    );
  };

  const filteredInitial  = filterRows(initial);
  const filteredIncoming = filterRows(incoming);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold">Inventory</h1>
        <input
          type="text"
          placeholder="Buscar producto, proveedor, tracking..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="px-3 py-1.5 border rounded text-sm w-72"
        />
      </div>

      <div className="flex gap-2 mb-6">
        <button onClick={() => setTab('initial')}
          className={`px-4 py-2 rounded font-medium text-sm ${tab === 'initial' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}>
          Inventario Inicial ({initial.length})
        </button>
        <button onClick={() => setTab('incoming')}
          className={`px-4 py-2 rounded font-medium text-sm ${tab === 'incoming' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}>
          Stock Pendiente / Recibido ({incoming.length})
        </button>
      </div>

      {tab === 'initial' && (
        <div className="bg-white border rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left">Producto</th>
                <th className="px-4 py-3 text-right">Cantidad</th>
                <th className="px-4 py-3 text-left">Fecha inicio</th>
              </tr>
            </thead>
            <tbody>
              {filteredInitial.map(r => (
                <tr key={r.id} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2">{r.product_name || <span className="text-gray-400 font-mono text-xs">{r.product_id.slice(0, 8)}…</span>}</td>
                  <td className="px-4 py-2 text-right font-medium">{r.quantity?.toLocaleString()}</td>
                  <td className="px-4 py-2 text-gray-500">{r.start_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredInitial.length === 0 && (
            <p className="p-4 text-center text-gray-400">
              {search ? 'Sin resultados para esa búsqueda.' : 'Sin inventario inicial. Sube el fichero en Importación.'}
            </p>
          )}
        </div>
      )}

      {tab === 'incoming' && (
        <div className="bg-white border rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left">Producto</th>
                <th className="px-4 py-3 text-right">Uds pedidas</th>
                <th className="px-4 py-3 text-left">Estado</th>
                <th className="px-4 py-3 text-left">Proveedor</th>
                <th className="px-4 py-3 text-left">Tracking</th>
                <th className="px-4 py-3 text-right">Coste/ud</th>
                <th className="px-4 py-3 text-left">Fecha pedido</th>
                <th className="px-4 py-3 text-left">Notas</th>
              </tr>
            </thead>
            <tbody>
              {filteredIncoming.map(r => (
                <tr key={r.id} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{r.product_name || <span className="text-gray-400 font-mono text-xs">{r.product_id.slice(0, 8)}…</span>}</td>
                  <td className="px-4 py-2 text-right">{r.qty_ordered?.toLocaleString()}</td>
                  <td className="px-4 py-2">{statusBadge(r.status)}</td>
                  <td className="px-4 py-2 text-gray-600">{r.supplier || '-'}</td>
                  <td className="px-4 py-2 font-mono text-xs text-gray-500">{r.tracking || '-'}</td>
                  <td className="px-4 py-2 text-right">{r.cost != null ? `$${r.cost.toFixed(2)}` : '-'}</td>
                  <td className="px-4 py-2 text-gray-500">{r.order_date || '-'}</td>
                  <td className="px-4 py-2 text-gray-400 text-xs max-w-xs truncate">{r.notes || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredIncoming.length === 0 && (
            <p className="p-4 text-center text-gray-400">
              {search ? 'Sin resultados para esa búsqueda.' : 'Sin stock pendiente. Sube el fichero en Importación.'}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
