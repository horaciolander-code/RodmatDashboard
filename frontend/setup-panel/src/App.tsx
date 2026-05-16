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

function ProtectedRoute({ children, roles }: { children: React.ReactNode; roles?: string[] }) {
  const { user, loading, isWarehouse } = useAuth();
  if (loading) return <div className="min-h-screen flex items-center justify-center text-gray-400">Cargando...</div>;
  if (!user) return <Navigate to="/login" />;
  if (roles && !roles.includes(user.role)) {
    return <Navigate to={isWarehouse ? '/inventory' : '/'} />;
  }
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
  const { user, isSuperadmin, isWarehouse, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => { logout(); navigate('/login'); };

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white border-b px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-5">
          <Link to={isWarehouse ? '/inventory' : '/'} className="font-bold text-lg text-blue-600">
            Rodmat V2
          </Link>

          {isSuperadmin && <StoreSwitcher />}

          {/* Warehouse: solo Inventario e Importación */}
          {isWarehouse ? (
            <>
              <Link to="/inventory" className="text-sm text-gray-600 hover:text-gray-900">Inventario</Link>
              <Link to="/import"    className="text-sm text-gray-600 hover:text-gray-900">Importación</Link>
            </>
          ) : (
            <>
              <Link to="/"          className="text-sm text-gray-600 hover:text-gray-900">Dashboard</Link>
              <Link to="/products"  className="text-sm text-gray-600 hover:text-gray-900">Productos</Link>
              <Link to="/combos"    className="text-sm text-gray-600 hover:text-gray-900">Combos</Link>
              <Link to="/inventory" className="text-sm text-gray-600 hover:text-gray-900">Inventario</Link>
              <Link to="/import"    className="text-sm text-gray-600 hover:text-gray-900">Importación</Link>
              <Link to="/settings"  className="text-sm text-gray-600 hover:text-gray-900">Ajustes</Link>
            </>
          )}
        </div>

        <div className="flex items-center gap-4">
          {isSuperadmin && (
            <span className="text-xs px-2 py-0.5 bg-purple-100 text-purple-700 rounded-full font-medium">superadmin</span>
          )}
          {isWarehouse && (
            <span className="text-xs px-2 py-0.5 bg-amber-100 text-amber-700 rounded-full font-medium">almacén</span>
          )}
          <span className="text-sm text-gray-500">{user?.email}</span>
          <button onClick={handleLogout} className="text-sm text-red-600 hover:underline">Salir</button>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-6 py-6">{children}</main>
    </div>
  );
}

const ALL_ROLES  = ['superadmin', 'admin', 'viewer', 'warehouse'];
const NO_WAREHOUSE = ['superadmin', 'admin', 'viewer'];

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/login"    element={<Login />} />
          <Route path="/register" element={<Register />} />

          {/* Rutas solo para roles NO warehouse */}
          <Route path="/" element={
            <ProtectedRoute roles={NO_WAREHOUSE}><Layout><Dashboard /></Layout></ProtectedRoute>
          } />
          <Route path="/products" element={
            <ProtectedRoute roles={NO_WAREHOUSE}><Layout><Products /></Layout></ProtectedRoute>
          } />
          <Route path="/combos" element={
            <ProtectedRoute roles={NO_WAREHOUSE}><Layout><Combos /></Layout></ProtectedRoute>
          } />
          <Route path="/settings" element={
            <ProtectedRoute roles={NO_WAREHOUSE}><Layout><Settings /></Layout></ProtectedRoute>
          } />

          {/* Rutas accesibles para todos (incluyendo warehouse) */}
          <Route path="/inventory" element={
            <ProtectedRoute roles={ALL_ROLES}><Layout><Inventory /></Layout></ProtectedRoute>
          } />
          <Route path="/import" element={
            <ProtectedRoute roles={ALL_ROLES}><Layout><DataImport /></Layout></ProtectedRoute>
          } />

          {/* Fallback: warehouse va a /inventory, resto a / */}
          <Route path="*" element={<RootRedirect />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}

function RootRedirect() {
  const { user, loading, isWarehouse } = useAuth();
  if (loading) return null;
  if (!user) return <Navigate to="/login" />;
  return <Navigate to={isWarehouse ? '/inventory' : '/'} />;
}
