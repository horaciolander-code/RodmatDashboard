import { useEffect, useState } from 'react';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

interface UserEntry {
  user_id: string;
  email: string;
  role: string;
  store_id: string;
}

function UserManager({ storeId }: { storeId: string }) {
  const [users, setUsers]         = useState<UserEntry[]>([]);
  const [email, setEmail]         = useState('');
  const [password, setPassword]   = useState('');
  const [role, setRole]           = useState<'warehouse' | 'viewer' | 'admin'>('warehouse');
  const [saving, setSaving]       = useState(false);
  const [msg, setMsg]             = useState<{ ok: boolean; text: string } | null>(null);

  const loadUsers = () => {
    api.get(`/admin/users?store_id=${storeId}`).then(r => setUsers(r.data)).catch(() => {});
  };

  useEffect(() => { loadUsers(); }, [storeId]);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setMsg(null);
    try {
      await api.post('/admin/users', { email, password, store_id: storeId, role });
      setMsg({ ok: true, text: `Usuario ${email} creado con rol ${role}.` });
      setEmail('');
      setPassword('');
      loadUsers();
    } catch (err: any) {
      const detail = err?.response?.data?.detail || 'Error al crear usuario.';
      setMsg({ ok: false, text: detail });
    }
    setSaving(false);
  };

  return (
    <div className="mt-10">
      <h2 className="text-lg font-semibold mb-4">Gestión de Usuarios</h2>

      {/* Lista actual */}
      <div className="bg-white border rounded-lg overflow-hidden mb-6">
        <table className="w-full text-sm">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left">Email</th>
              <th className="px-4 py-2 text-left">Rol</th>
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.user_id} className="border-t">
                <td className="px-4 py-2">{u.email}</td>
                <td className="px-4 py-2">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                    u.role === 'superadmin' ? 'bg-purple-100 text-purple-700' :
                    u.role === 'admin'      ? 'bg-blue-100 text-blue-700' :
                    u.role === 'warehouse'  ? 'bg-amber-100 text-amber-700' :
                                             'bg-gray-100 text-gray-600'
                  }`}>{u.role}</span>
                </td>
              </tr>
            ))}
            {users.length === 0 && (
              <tr><td colSpan={2} className="px-4 py-3 text-center text-gray-400">Sin usuarios</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Formulario nuevo usuario */}
      <form onSubmit={handleCreate} className="max-w-lg space-y-4 bg-white border rounded-lg p-6">
        <h3 className="font-medium text-gray-800">Crear nuevo usuario</h3>
        <div>
          <label className="block text-sm font-medium mb-1">Email</label>
          <input
            type="email" required value={email}
            onChange={e => setEmail(e.target.value)}
            className="w-full px-3 py-2 border rounded text-sm"
            placeholder="almacen@rodmat.com"
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Contraseña (mín. 8 caracteres)</label>
          <input
            type="password" required minLength={8} value={password}
            onChange={e => setPassword(e.target.value)}
            className="w-full px-3 py-2 border rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Rol</label>
          <select value={role} onChange={e => setRole(e.target.value as any)}
            className="w-full px-3 py-2 border rounded text-sm">
            <option value="warehouse">warehouse — solo inventario e importación</option>
            <option value="viewer">viewer — solo lectura</option>
            <option value="admin">admin — acceso completo</option>
          </select>
        </div>
        <button type="submit" disabled={saving}
          className="w-full bg-blue-600 text-white py-2 rounded hover:bg-blue-700 disabled:opacity-50 text-sm font-medium">
          {saving ? 'Creando...' : 'Crear usuario'}
        </button>
        {msg && (
          <p className={`text-sm text-center ${msg.ok ? 'text-green-600' : 'text-red-500'}`}>{msg.text}</p>
        )}
      </form>
    </div>
  );
}

export default function Settings() {
  const { isSuperadmin, activeStoreId } = useAuth();
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

      {isSuperadmin && <UserManager storeId={activeStoreId} />}
    </div>
  );
}
