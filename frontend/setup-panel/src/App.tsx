import { useState, useRef, useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate, Link, useNavigate } from 'react-router-dom';
import { AuthProvider, useAuth, type StoreOption } from './context/AuthContext';
import Login from './pages/Login';
import Register from './pages/Register';
import Dashboard from './pages/Dashboard';
import Products from './pages/Products';
import Combos from './pages/Combos';
import Inventory from './pages/Inventory';
import DataImport from './pages/DataImport';
import Settings from './pages/Settings';

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth();
  if (loading) return <div className="min-h-screen flex items-center justify-center">Loading...</div>;
  if (!user) return <Navigate to="/login" />;
  return <>{children}</>;
}

function StoreSwitcher() {
  const { stores, activeStoreName, setActiveStore } = useAuth();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 px-3 py-1.5 bg-blue-50 border border-blue-200 rounded-md text-sm font-medium text-blue-700 hover:bg-blue-100"
      >
        <span className="text-xs text-blue-400">Tienda:</span>
        <span>{activeStoreName || '...'}</span>
        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="absolute left-0 top-full mt-1 w-60 bg-white border rounded-lg shadow-lg z-50">
          {stores.map((s: StoreOption) => (
            <button
              key={s.id}
              onClick={() => { setActiveStore(s); setOpen(false); }}
              className={`w-full text-left px-4 py-2.5 text-sm hover:bg-gray-50 first:rounded-t-lg last:rounded-b-lg ${
                activeStoreName === s.name ? 'font-semibold text-blue-600 bg-blue-50' : 'text-gray-700'
              }`}
            >
              <div>{s.name}</div>
              <div className="text-xs text-gray-400">{s.owner_email}</div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function Layout({ children }: { children: React.ReactNode }) {
  const { user, isSuperadmin, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => { logout(); navigate('/login'); };

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white border-b px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-5">
          <Link to="/" className="font-bold text-lg text-blue-600">Rodmat V2</Link>
          {isSuperadmin && <StoreSwitcher />}
          <Link to="/" className="text-sm text-gray-600 hover:text-gray-900">Dashboard</Link>
          <Link to="/products" className="text-sm text-gray-600 hover:text-gray-900">Products</Link>
          <Link to="/combos" className="text-sm text-gray-600 hover:text-gray-900">Combos</Link>
          <Link to="/inventory" className="text-sm text-gray-600 hover:text-gray-900">Inventory</Link>
          <Link to="/import" className="text-sm text-gray-600 hover:text-gray-900">Import</Link>
          <Link to="/settings" className="text-sm text-gray-600 hover:text-gray-900">Settings</Link>
        </div>
        <div className="flex items-center gap-4">
          {isSuperadmin && (
            <span className="text-xs px-2 py-0.5 bg-purple-100 text-purple-700 rounded-full font-medium">
              superadmin
            </span>
          )}
          <span className="text-sm text-gray-500">{user?.email}</span>
          <button onClick={handleLogout} className="text-sm text-red-600 hover:underline">Logout</button>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-6 py-6">{children}</main>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          <Route path="/" element={<ProtectedRoute><Layout><Dashboard /></Layout></ProtectedRoute>} />
          <Route path="/products" element={<ProtectedRoute><Layout><Products /></Layout></ProtectedRoute>} />
          <Route path="/combos" element={<ProtectedRoute><Layout><Combos /></Layout></ProtectedRoute>} />
          <Route path="/inventory" element={<ProtectedRoute><Layout><Inventory /></Layout></ProtectedRoute>} />
          <Route path="/import" element={<ProtectedRoute><Layout><DataImport /></Layout></ProtectedRoute>} />
          <Route path="/settings" element={<ProtectedRoute><Layout><Settings /></Layout></ProtectedRoute>} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}
