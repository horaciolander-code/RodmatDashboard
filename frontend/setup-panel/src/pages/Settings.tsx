import { useEffect, useState } from 'react';
import api from '../services/api';

export default function Settings() {
  const [store, setStore] = useState<any>(null);
  const [form, setForm] = useState({
    name: '',
    currency: 'USD',
    timezone: 'America/New_York',
    initial_inventory_date: '2026-01-01',
    report_recipients: '',
    report_enabled: true,
    low_stock_threshold: 30,
  });
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    api.get('/stores/me').then(r => {
      const s = r.data;
      setStore(s);
      setForm({
        name: s.name || '',
        currency: s.currency || 'USD',
        timezone: s.timezone || 'America/New_York',
        initial_inventory_date: s.settings?.initial_inventory_date || '2026-01-01',
        report_recipients: (s.settings?.report_recipients || []).join(', '),
        report_enabled: s.settings?.report_enabled ?? true,
        low_stock_threshold: s.settings?.low_stock_threshold || 30,
      });
    }).catch(() => {});
  }, []);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    const settings = {
      initial_inventory_date: form.initial_inventory_date,
      report_recipients: form.report_recipients.split(',').map(s => s.trim()).filter(Boolean),
      report_enabled: form.report_enabled,
      low_stock_threshold: form.low_stock_threshold,
    };
    await api.put('/stores/me', {
      name: form.name,
      currency: form.currency,
      timezone: form.timezone,
      settings,
    });
    setSaved(true);
    setTimeout(() => setSaved(false), 3000);
  };

  if (!store) return <p>Loading...</p>;

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Store Settings</h1>

      <form onSubmit={handleSave} className="max-w-lg space-y-4 bg-white border rounded-lg p-6">
        <div>
          <label className="block text-sm font-medium mb-1">Store Name</label>
          <input value={form.name} onChange={e => setForm({...form, name: e.target.value})}
            className="w-full px-3 py-2 border rounded" />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Currency</label>
            <select value={form.currency} onChange={e => setForm({...form, currency: e.target.value})}
              className="w-full px-3 py-2 border rounded">
              <option>USD</option><option>EUR</option><option>GBP</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Timezone</label>
            <input value={form.timezone} onChange={e => setForm({...form, timezone: e.target.value})}
              className="w-full px-3 py-2 border rounded" />
          </div>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Initial Inventory Date</label>
          <input type="date" value={form.initial_inventory_date}
            onChange={e => setForm({...form, initial_inventory_date: e.target.value})}
            className="w-full px-3 py-2 border rounded" />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Report Recipients (comma-separated emails)</label>
          <input value={form.report_recipients}
            onChange={e => setForm({...form, report_recipients: e.target.value})}
            className="w-full px-3 py-2 border rounded" placeholder="email1@example.com, email2@example.com" />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Low Stock Threshold (days)</label>
            <input type="number" min={1} value={form.low_stock_threshold}
              onChange={e => setForm({...form, low_stock_threshold: parseInt(e.target.value) || 30})}
              className="w-full px-3 py-2 border rounded" />
          </div>
          <div className="flex items-end">
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={form.report_enabled}
                onChange={e => setForm({...form, report_enabled: e.target.checked})} />
              <span className="text-sm">Enable Daily Reports</span>
            </label>
          </div>
        </div>

        <button type="submit" className="w-full bg-blue-600 text-white py-2 rounded hover:bg-blue-700">
          Save Settings
        </button>
        {saved && <p className="text-green-600 text-sm text-center">Settings saved!</p>}
      </form>
    </div>
  );
}
