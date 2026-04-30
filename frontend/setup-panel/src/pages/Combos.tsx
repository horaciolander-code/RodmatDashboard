import { useEffect, useState } from 'react';
import api from '../services/api';

interface Product { id: string; sku: string; name: string; }
interface ComboItem { id: string; product_id: string; quantity: number; }
interface Combo { id: string; combo_sku: string; combo_name: string; items: ComboItem[]; }

export default function Combos() {
  const [combos, setCombos] = useState<Combo[]>([]);
  const [products, setProducts] = useState<Product[]>([]);
  const [form, setForm] = useState({ combo_sku: '', combo_name: '' });
  const [selectedProducts, setSelectedProducts] = useState<{ product_id: string; quantity: number }[]>([]);

  const load = () => {
    api.get('/combos').then(r => setCombos(r.data)).catch(() => {});
    api.get('/products').then(r => setProducts(r.data)).catch(() => {});
  };
  useEffect(() => { load(); }, []);

  const addProductRow = () => setSelectedProducts([...selectedProducts, { product_id: '', quantity: 1 }]);
  const removeProductRow = (i: number) => setSelectedProducts(selectedProducts.filter((_, idx) => idx !== i));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const items = selectedProducts.filter(p => p.product_id);
    await api.post('/combos', { ...form, items });
    setForm({ combo_sku: '', combo_name: '' });
    setSelectedProducts([]);
    load();
  };

  const productMap = Object.fromEntries(products.map(p => [p.id, p]));

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Combos</h1>

      <form onSubmit={handleSubmit} className="mb-6 p-4 bg-white border rounded-lg space-y-3">
        <div className="grid grid-cols-2 gap-3">
          <input placeholder="Combo SKU *" value={form.combo_sku}
            onChange={e => setForm({...form, combo_sku: e.target.value})} required
            className="px-3 py-2 border rounded" />
          <input placeholder="Combo Name *" value={form.combo_name}
            onChange={e => setForm({...form, combo_name: e.target.value})} required
            className="px-3 py-2 border rounded" />
        </div>

        <p className="font-semibold text-sm">Components:</p>
        {selectedProducts.map((sp, i) => (
          <div key={i} className="flex gap-2 items-center">
            <select value={sp.product_id}
              onChange={e => { const arr = [...selectedProducts]; arr[i].product_id = e.target.value; setSelectedProducts(arr); }}
              className="flex-1 px-3 py-2 border rounded">
              <option value="">Select product...</option>
              {products.map(p => <option key={p.id} value={p.id}>{p.sku} - {p.name}</option>)}
            </select>
            <input type="number" min={1} value={sp.quantity}
              onChange={e => { const arr = [...selectedProducts]; arr[i].quantity = parseInt(e.target.value) || 1; setSelectedProducts(arr); }}
              className="w-20 px-3 py-2 border rounded" />
            <button type="button" onClick={() => removeProductRow(i)} className="text-red-500 px-2">X</button>
          </div>
        ))}
        <button type="button" onClick={addProductRow} className="text-blue-600 text-sm hover:underline">
          + Add component
        </button>

        <button type="submit" className="block w-full bg-blue-600 text-white py-2 rounded hover:bg-blue-700">
          Create Combo
        </button>
      </form>

      <div className="space-y-3">
        {combos.map(c => (
          <div key={c.id} className="bg-white border rounded-lg p-4">
            <h3 className="font-semibold">{c.combo_sku} - {c.combo_name}</h3>
            <ul className="mt-2 text-sm text-gray-600">
              {c.items.map(item => (
                <li key={item.id}>
                  {productMap[item.product_id]?.name || item.product_id} x{item.quantity}
                </li>
              ))}
            </ul>
          </div>
        ))}
        {combos.length === 0 && <p className="text-gray-400 text-center p-4">No combos yet</p>}
      </div>
    </div>
  );
}
