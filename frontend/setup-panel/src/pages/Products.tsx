import { useEffect, useState } from 'react';
import api from '../services/api';

interface Product {
  id: string;
  sku: string;
  name: string;
  category: string | null;
  price_sale: number | null;
  price_cost: number | null;
  units_per_box: number | null;
  status: string;
}

export default function Products() {
  const [products, setProducts] = useState<Product[]>([]);
  const [form, setForm] = useState({ sku: '', name: '', category: '', price_sale: '', price_cost: '', units_per_box: '' });

  const load = () => api.get('/products').then(r => setProducts(r.data)).catch(() => {});
  useEffect(() => { load(); }, []);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    await api.post('/products', {
      sku: form.sku, name: form.name, category: form.category || null,
      price_sale: form.price_sale ? parseFloat(form.price_sale) : null,
      price_cost: form.price_cost ? parseFloat(form.price_cost) : null,
      units_per_box: form.units_per_box ? parseInt(form.units_per_box) : null,
    });
    setForm({ sku: '', name: '', category: '', price_sale: '', price_cost: '', units_per_box: '' });
    load();
  };

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Products</h1>

      <form onSubmit={handleAdd} className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6 p-4 bg-white border rounded-lg">
        <input placeholder="SKU *" value={form.sku} onChange={e => setForm({...form, sku: e.target.value})} required
          className="px-3 py-2 border rounded" />
        <input placeholder="Name *" value={form.name} onChange={e => setForm({...form, name: e.target.value})} required
          className="px-3 py-2 border rounded" />
        <input placeholder="Category" value={form.category} onChange={e => setForm({...form, category: e.target.value})}
          className="px-3 py-2 border rounded" />
        <input placeholder="Sale Price" type="number" step="0.01" value={form.price_sale}
          onChange={e => setForm({...form, price_sale: e.target.value})} className="px-3 py-2 border rounded" />
        <input placeholder="Cost Price" type="number" step="0.01" value={form.price_cost}
          onChange={e => setForm({...form, price_cost: e.target.value})} className="px-3 py-2 border rounded" />
        <input placeholder="Units/Box" type="number" value={form.units_per_box}
          onChange={e => setForm({...form, units_per_box: e.target.value})} className="px-3 py-2 border rounded" />
        <button type="submit" className="bg-blue-600 text-white py-2 rounded hover:bg-blue-700 col-span-2">
          Add Product
        </button>
      </form>

      <div className="bg-white border rounded-lg overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left">SKU</th>
              <th className="px-4 py-3 text-left">Name</th>
              <th className="px-4 py-3 text-left">Category</th>
              <th className="px-4 py-3 text-right">Sale $</th>
              <th className="px-4 py-3 text-right">Cost $</th>
              <th className="px-4 py-3 text-right">Units/Box</th>
              <th className="px-4 py-3 text-left">Status</th>
            </tr>
          </thead>
          <tbody>
            {products.map(p => (
              <tr key={p.id} className="border-t hover:bg-gray-50">
                <td className="px-4 py-2 font-mono">{p.sku}</td>
                <td className="px-4 py-2">{p.name}</td>
                <td className="px-4 py-2">{p.category || '-'}</td>
                <td className="px-4 py-2 text-right">{p.price_sale?.toFixed(2) ?? '-'}</td>
                <td className="px-4 py-2 text-right">{p.price_cost?.toFixed(2) ?? '-'}</td>
                <td className="px-4 py-2 text-right">{p.units_per_box ?? '-'}</td>
                <td className="px-4 py-2">{p.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {products.length === 0 && <p className="p-4 text-gray-400 text-center">No products yet</p>}
      </div>
    </div>
  );
}
