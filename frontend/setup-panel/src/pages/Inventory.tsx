import { useEffect, useState } from 'react';
import api from '../services/api';

interface InvRecord {
  id: string;
  product_id: string;
  quantity?: number;
  qty_ordered?: number;
  start_date?: string;
  status?: string;
  supplier?: string;
  notes?: string;
}

export default function Inventory() {
  const [tab, setTab] = useState<'initial' | 'incoming'>('initial');
  const [initial, setInitial] = useState<InvRecord[]>([]);
  const [incoming, setIncoming] = useState<InvRecord[]>([]);

  const load = () => {
    api.get('/inventory/initial').then(r => setInitial(r.data)).catch(() => {});
    api.get('/inventory/incoming').then(r => setIncoming(r.data)).catch(() => {});
  };
  useEffect(() => { load(); }, []);

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Inventory</h1>

      <div className="flex gap-2 mb-6">
        <button onClick={() => setTab('initial')}
          className={`px-4 py-2 rounded ${tab === 'initial' ? 'bg-blue-600 text-white' : 'bg-gray-200'}`}>
          Initial Inventory ({initial.length})
        </button>
        <button onClick={() => setTab('incoming')}
          className={`px-4 py-2 rounded ${tab === 'incoming' ? 'bg-blue-600 text-white' : 'bg-gray-200'}`}>
          Incoming Stock ({incoming.length})
        </button>
      </div>

      {tab === 'initial' && (
        <div className="bg-white border rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left">Product ID</th>
                <th className="px-4 py-3 text-right">Quantity</th>
                <th className="px-4 py-3 text-left">Start Date</th>
              </tr>
            </thead>
            <tbody>
              {initial.map(r => (
                <tr key={r.id} className="border-t">
                  <td className="px-4 py-2 font-mono text-xs">{r.product_id.slice(0, 8)}...</td>
                  <td className="px-4 py-2 text-right">{r.quantity}</td>
                  <td className="px-4 py-2">{r.start_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {initial.length === 0 && <p className="p-4 text-center text-gray-400">No initial inventory. Upload via Import.</p>}
        </div>
      )}

      {tab === 'incoming' && (
        <div className="bg-white border rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left">Product ID</th>
                <th className="px-4 py-3 text-right">Qty Ordered</th>
                <th className="px-4 py-3 text-left">Status</th>
                <th className="px-4 py-3 text-left">Supplier</th>
                <th className="px-4 py-3 text-left">Notes</th>
              </tr>
            </thead>
            <tbody>
              {incoming.map(r => (
                <tr key={r.id} className="border-t">
                  <td className="px-4 py-2 font-mono text-xs">{r.product_id.slice(0, 8)}...</td>
                  <td className="px-4 py-2 text-right">{r.qty_ordered}</td>
                  <td className="px-4 py-2">
                    <span className={`px-2 py-1 rounded text-xs ${
                      r.status === 'Recibido' ? 'bg-green-100 text-green-700' :
                      r.status === 'pending' ? 'bg-yellow-100 text-yellow-700' : 'bg-gray-100'
                    }`}>{r.status}</span>
                  </td>
                  <td className="px-4 py-2">{r.supplier || '-'}</td>
                  <td className="px-4 py-2">{r.notes || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {incoming.length === 0 && <p className="p-4 text-center text-gray-400">No incoming stock. Upload via Import.</p>}
        </div>
      )}
    </div>
  );
}
